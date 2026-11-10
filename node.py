import json
import os
import queue
import random
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NODE_ID = os.environ.get("NODE_ID", "node1")
PORT = int(os.environ.get("PORT", "8000"))
INITIAL_FATHER = os.environ.get("INITIAL_FATHER", "") or None
HAS_TOKEN_INIT = os.environ.get("HAS_TOKEN", "false").lower() == "true"
OBSERVER_URL = os.environ.get("OBSERVER_URL", "http://observer:9000")
SYNC_ENABLED = os.environ.get("SYNC_ENABLED", "true").lower() == "true"
CS_MIN_SLEEP = float(os.environ.get("CS_MIN_SLEEP", "0.2"))
CS_MAX_SLEEP = float(os.environ.get("CS_MAX_SLEEP", "1.0"))

# Peers mapeados por nome de servico DNS do Docker
PEERS = {
    f"node{i}": f"http://node{i}:8000" for i in range(1, 11)
}

class NaimiTrehelNode:
    def __init__(self):
        self.state_lock = threading.Lock()
        self.client_entry_lock = threading.Lock()
        self.father = INITIAL_FATHER
        self.next_queue = []
        self.requesting = False
        self.has_token = HAS_TOKEN_INIT
        self.token_event = threading.Event()
        if self.has_token:
            self.token_event.set()

        self.protocol_msgs_sent = 0
        self.acquisitions = 0
        self.wait_times_ms = []

        # Filas de envio FIFO por destinatario para garantir canais FIFO
        self.send_queues = {}
        for peer_id in PEERS:
            if peer_id != NODE_ID:
                q = queue.Queue()
                self.send_queues[peer_id] = q
                t = threading.Thread(target=self._sender_worker, args=(peer_id, q), daemon=True)
                t.start()

    def _sender_worker(self, target_id, q):
        while True:
            endpoint, payload = q.get()
            url = f"{PEERS[target_id]}{endpoint}"
            body = json.dumps(payload).encode("utf-8")
            while True:
                try:
                    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        resp.read()
                    break
                except Exception as e:
                    time.sleep(0.05)
            q.task_done()

    def send_async(self, target_id, endpoint, payload):
        self.protocol_msgs_sent += 1
        self.send_queues[target_id].put((endpoint, payload))

    def notify_observer(self, event_type, request_id):
        body = json.dumps({
            "event": event_type,
            "node_id": NODE_ID,
            "request_id": request_id
        }).encode("utf-8")
        req = urllib.request.Request(f"{OBSERVER_URL}/event", data=body, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()

    def log_event(self, stage, request_id, extra=""):
        print(
            f"[{stage:<9}] node={NODE_ID} | req={request_id} | token={self.has_token} "
            f"| father={self.father} | next={self.next_queue} {extra}",
            flush=True
        )

    def acquire(self, resource, request_id):
        start_mono = time.monotonic()
        if not SYNC_ENABLED:
            self.log_event("REQUEST", request_id, "(SYNC_DISABLED)")
            self.log_event("ACQUIRE", request_id, "(SYNC_DISABLED)")
            return (time.monotonic() - start_mono) * 1000.0

        with self. state_lock:
            self.requesting = True
            self.log_event("REQUEST", request_id)
            if self.father is not None:
                target = self.father
                self.father = None
                self.token_event.clear()
                self.send_async(target, "/protocol/request", {"requester": NODE_ID, "request_id": request_id})
            else:
                self.token_event.set()

        self.token_event.wait()
        wait_ms = (time.monotonic() - start_mono) * 1000.0

        with self.state_lock:
            self.acquisitions += 1
            self.wait_times_ms.append(wait_ms)
            self.log_event("ACQUIRE", request_id, f"| wait_ms={wait_ms:.2f}")
        return wait_ms

    def release(self, resource, request_id):
        if not SYNC_ENABLED:
            self.log_event("RELEASE", request_id, "(SYNC_DISABLED)")
            return

        with self.state_lock:
            self.requesting = False
            if len(self.next_queue) > 0:
                next_node = self.next_queue.pop(0)
                self.has_token = False
                self.token_event.clear()
                self.log_event("RELEASE", request_id, f"-> enviando token para {next_node}")
                self.send_async(next_node, "/protocol/token", {"from": NODE_ID, "request_id": request_id})
            else:
                self.log_event("RELEASE", request_id, "-> mantendo token local")

    def on_receive_request(self, requester, request_id):
        with self.state_lock:
            prev_father = self.father
            if self.father is None:
                if self.requesting:
                    self.next_queue.append(requester)
                else:
                    self.has_token = False
                    self.token_event.clear()
                    self.send_async(requester, "/protocol/token", {"from": NODE_ID, "request_id": request_id})
            else:
                self.send_async(self.father, "/protocol/request", {"requester": requester, "request_id": request_id})
            self.father = requester
            print(
                f"[REV_PATH ] node={NODE_ID} | req_from={requester} | father: {prev_father} -> {self.father} | next={self.next_queue}",
                flush=True
            )

    def on_receive_token(self, sender, request_id):
        with self.state_lock:
            self.has_token = True
            self.token_event.set()

    def handle_client_write(self, resource, request_id):
        # Serializa pedidos concorrentes recebidos pelo mesmo no
        with self.client_entry_lock:
            wait_ms = self.acquire(resource, request_id)
            self.log_event("ENTER", request_id)
            self.notify_observer("BEGIN", request_id)
            time.sleep(random.uniform(CS_MIN_SLEEP, CS_MAX_SLEEP))
            self.notify_observer("END", request_id)
            self.log_event("EXIT", request_id)
            self.release(resource, request_id)
            self.log_event("COMMITTED", request_id)
            return wait_ms

node = NaimiTrehelNode()

class NodeHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        data = json.loads(self.rfile.read(length).decode("utf-8"))

        if self.path == "/client/request":
            wait_ms = node.handle_client_write(data.get("resource", "R"), data["request_id"])
            resp = {"status": "COMMITTED", "node_id": NODE_ID, "request_id": data["request_id"], "wait_ms": wait_ms}
        elif self.path == "/protocol/request":
            node.on_receive_request(data["requester"], data["request_id"])
            resp = {"status": "OK"}
        elif self.path == "/protocol/token":
            node.on_receive_token(data["from"], data["request_id"])
            resp = {"status": "OK"}
        else:
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(resp).encode("utf-8"))

    def do_GET(self):
        if self.path == "/status":
            with node.state_lock:
                sorted_waits = sorted(node.wait_times_ms)
                median_wait = sorted_waits[len(sorted_waits) // 2] if sorted_waits else 0.0
                payload = {
                    "node_id": NODE_ID,
                    "father": node.father,
                    "next_queue": node.next_queue,
                    "requesting": node.requesting,
                    "has_token": node.has_token,
                    "protocol_msgs_sent": node.protocol_msgs_sent,
                    "acquisitions": node.acquisitions,
                    "median_wait_ms": round(median_wait, 2)
                }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload, indent=2).encode("utf-8"))

if __name__ == "__main__":
    print(f"[INIT] {NODE_ID} iniciado | father={node.father} | token={node.has_token} | sync={SYNC_ENABLED}", flush=True)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), NodeHandler)
    server.serve_forever()
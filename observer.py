import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

lock = threading.Lock()
active_nodes = set()
total_completed = 0
overlap_events = 0
max_concurrent = 0
history = []

class ObserverHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_POST(self):
        global total_completed, overlap_events, max_concurrent
        length = int(self.headers.get("Content-Length", 0))
        data = json.loads(self.rfile.read(length).decode("utf-8"))
        event_type = data["event"]
        node_id = data["node_id"]
        req_id = data["request_id"]
        ts = time.time()

        with lock:
            if event_type == "BEGIN":
                active_nodes.add(node_id)
                current_count = len(active_nodes)
                if current_count > max_concurrent:
                    max_concurrent = current_count
                if current_count > 1:
                    overlap_events += 1
                    print(f"[VIOLACAO] Sobreposicao detectada! Ativos: {list(active_nodes)} | req={req_id}", flush=True)
                else:
                    print(f"[OBSERVER] BEGIN | node={node_id} | req={req_id} | ativos={current_count}", flush=True)
            elif event_type == "END":
                active_nodes.discard(node_id)
                total_completed += 1
                print(f"[OBSERVER] END   | node={node_id} | req={req_id} | ativos={len(active_nodes)}", flush=True)

            history.append({"ts": ts, "event": event_type, "node": node_id, "req_id": req_id, "active": len(active_nodes)})

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok"}).encode("utf-8"))

    def do_GET(self):
        if self.path == "/stats":
            with lock:
                payload = {
                    "active_now": list(active_nodes),
                    "total_completed": total_completed,
                    "overlap_events": overlap_events,
                    "max_concurrent": max_concurrent
                }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload, indent=2).encode("utf-8"))

if __name__ == "__main__":
    print("[OBSERVER] Iniciando na porta 9000...", flush=True)
    server = ThreadingHTTPServer(("0.0.0.0", 9000), ObserverHandler)
    server.serve_forever()
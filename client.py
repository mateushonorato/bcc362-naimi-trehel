import json
import os
import random
import time
import urllib.request

CLIENT_ID = os.environ.get("CLIENT_ID", "client1")
TARGET_URL = os.environ.get("TARGET_URL", "http://node1:8000")
NUM_REQUESTS = int(os.environ.get("NUM_REQUESTS", "10"))
MIN_SLEEP = float(os.environ.get("CLIENT_MIN_SLEEP", "1.0"))
MAX_SLEEP = float(os.environ.get("CLIENT_MAX_SLEEP", "5.0"))
RANDOM_SEED = int(os.environ.get("RANDOM_SEED", "42"))

random.seed(f"{RANDOM_SEED}-{CLIENT_ID}")

def send_with_retry(url, body):
    while True:
        try:
            req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            time.sleep(1.0)

if __name__ == "__main__":
    print(f"[{CLIENT_ID}] Iniciando {NUM_REQUESTS} pedidos para {TARGET_URL}", flush=True)
    for seq in range(1, NUM_REQUESTS + 1):
        req_id = f"{CLIENT_ID}-{seq:03d}"
        body = json.dumps({"resource": "R", "request_id": req_id}).encode("utf-8")
        result = send_with_retry(f"{TARGET_URL}/client/request", body)
        print(f"[{CLIENT_ID}] {req_id} -> {result['status']} (espera={result['wait_ms']:.2f}ms)", flush=True)
        if seq < NUM_REQUESTS and MAX_SLEEP > 0:
            time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))
    print(f"[{CLIENT_ID}] Concluiu todos os {NUM_REQUESTS} pedidos.", flush=True)
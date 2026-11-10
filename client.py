"""
client.py — Gerador de Carga do Cluster Sync
BCC362 — Sistemas Distribuídos | UFOP | Professor Joubert Lima

Cada instância deste script representa um cliente independente que envia
pedidos sequenciais de escrita ao recurso compartilhado R, sempre por meio
de um único nó do Cluster Sync (seu "ponto de entrada").

Comportamento conforme o enunciado (Seção 2 do PDF):
  - Cada pedido é identificado de forma única por request_id = (CLIENT_ID, seq).
  - O cliente aguarda o COMMITTED do nó (bloqueio síncrono) antes de continuar.
  - Entre pedidos, dorme um intervalo aleatório de CLIENT_MIN_SLEEP a CLIENT_MAX_SLEEP.
  - Alta Contenção: CLIENT_MIN_SLEEP = CLIENT_MAX_SLEEP = 0 (pedidos imediatos).
  - Carga Desigual: client1 usa intervalo de 0,1 a 0,3 s; demais usam 1 a 5 s.
"""

import json
import os
import random
import time
import urllib.request

# ---------------------------------------------------------------------------
# Configuração via variáveis de ambiente (injetadas pelo Docker Compose/Swarm)
# ---------------------------------------------------------------------------
CLIENT_ID     = os.environ.get("CLIENT_ID", "client1")
TARGET_URL    = os.environ.get("TARGET_URL", "http://node1:8000")
NUM_REQUESTS  = int(os.environ.get("NUM_REQUESTS", "10"))    # 10 a 50 conforme enunciado
MIN_SLEEP     = float(os.environ.get("CLIENT_MIN_SLEEP", "1.0"))
MAX_SLEEP     = float(os.environ.get("CLIENT_MAX_SLEEP", "5.0"))
RANDOM_SEED   = int(os.environ.get("RANDOM_SEED", "42"))

# Timeout da requisição HTTP: o cliente pode ficar bloqueado por longos períodos
# na alta contenção (todos os nós disputando), por isso o default é 300 s.
CLIENT_TIMEOUT = float(os.environ.get("CLIENT_TIMEOUT", "300.0"))

# Inicialização da semente com (RANDOM_SEED, CLIENT_ID) para garantir que cada
# cliente tenha uma sequência pseudoaleatória reproduzível e distinta dos demais.
random.seed(f"{RANDOM_SEED}-{CLIENT_ID}")


def send_with_retry(url, body):
    """Envia uma requisição HTTP POST e retenta indefinidamente em caso de falha.

    O cliente não desiste: se o nó alvo ainda estiver inicializando ou sofrendo
    atraso de rede, aguarda 1 segundo e tenta novamente. Isso é compatível com
    o pressuposto de canal confiável sem quedas do enunciado do TP2.

    Retorna o JSON da resposta quando bem-sucedido.
    """
    while True:
        try:
            req = urllib.request.Request(
                url, data=body,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=CLIENT_TIMEOUT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            time.sleep(1.0)


if __name__ == "__main__":
    print(f"[{CLIENT_ID}] Iniciando {NUM_REQUESTS} pedidos para {TARGET_URL}", flush=True)

    for seq in range(1, NUM_REQUESTS + 1):
        # request_id único no formato (client_id, número_sequencial com zero-padding)
        # Exemplo: "client1-001", "client3-042"
        req_id = f"{CLIENT_ID}-{seq:03d}"
        body   = json.dumps({"resource": "R", "request_id": req_id}).encode("utf-8")

        # Bloqueia até receber COMMITTED do nó (que só responde após sair da SC)
        result = send_with_retry(f"{TARGET_URL}/client/request", body)
        print(
            f"[{CLIENT_ID}] {req_id} -> {result['status']} (espera={result['wait_ms']:.2f}ms)",
            flush=True
        )

        # Aguarda o intervalo aleatório entre pedidos (não aplicado após o último)
        if seq < NUM_REQUESTS and MAX_SLEEP > 0:
            time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))

    print(f"[{CLIENT_ID}] Concluiu todos os {NUM_REQUESTS} pedidos.", flush=True)

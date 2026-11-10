"""
observer.py — Verificador Passivo de Exclusão Mútua
BCC362 — Sistemas Distribuídos | UFOP | Professor Joubert Lima

Implementa o observador passivo exigido pela Seção 7 do enunciado do TP2.
Recebe notificações BEGIN e END de todos os nós ao entrarem e saírem da
seção crítica simulada, e mantém a contagem de acessos ativos a R.

Propriedade verificada: exclusão mútua — em qualquer instante, no máximo
UM processo deve estar executando a seção crítica (active_entries.size <= 1).

IMPORTANTE: O observador é um instrumento de medição, NÃO um mecanismo de
autorização. Ele apenas registra e detecta sobreposições; a permissão de
entrada é concedida exclusivamente pelo protocolo Naimi–Tréhel dos nós.
Isso permite verificar a propriedade sem depender de comparação de relógios
físicos entre máquinas distintas (cada BEGIN/END é processado na ordem de
chegada ao observador).

Endpoints expostos:
  POST /event  — recebe eventos BEGIN e END dos nós.
  GET  /stats  — retorna métricas agregadas (consultado por collect_metrics.py).
"""

import json
import os
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "http://dashboard:8080")

def notify_dashboard(payload):
    """Envia evento ao Dashboard Web de forma assíncrona sem bloquear o observador."""
    if not DASHBOARD_URL:
        return
    def _send():
        try:
            body = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                f"{DASHBOARD_URL}/event", data=body,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=1.0) as resp:
                resp.read()
        except Exception:
            pass
    threading.Thread(target=_send, daemon=True).start()

# ---------------------------------------------------------------------------
# Estado global do observador (protegido por lock para acesso thread-safe)
# ---------------------------------------------------------------------------

lock = threading.Lock()

# Conjunto de (node_id, req_id) atualmente dentro da seção crítica.
# Se len(active_entries) > 1 ao receber um BEGIN, há sobreposição (violação).
active_entries = set()

total_completed = 0   # Total de acessos completados (recebeu END)
overlap_events  = 0   # Número de BEGIN recebidos com outro processo já ativo (violações)
max_concurrent  = 0   # Máximo de processos simultâneos observado

# Histórico circular dos últimos MAX_HISTORY eventos (para diagnóstico e replay)
history     = []
MAX_HISTORY = 1000


class ObserverHandler(BaseHTTPRequestHandler):
    """Handler HTTP do observador."""

    def log_message(self, format, *args):
        # Suprime os logs padrão do servidor HTTP para manter o stdout limpo.
        return

    def do_POST(self):
        global total_completed, overlap_events, max_concurrent

        if self.path == "/reset":
            with lock:
                active_entries.clear()
                total_completed = 0
                overlap_events = 0
                max_concurrent = 0
                history.clear()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode("utf-8"))
            return

        length     = int(self.headers.get("Content-Length", 0))
        data       = json.loads(self.rfile.read(length).decode("utf-8"))
        event_type = data["event"]       # "BEGIN" ou "END"
        node_id    = data["node_id"]
        req_id     = data["request_id"]
        ts         = time.time()         # timestamp físico apenas para o histórico interno

        with lock:
            if event_type == "BEGIN":
                # Um nó entrou na seção crítica: registra no conjunto de ativos
                active_entries.add((node_id, req_id))
                current_count = len(active_entries)

                # Atualiza o pico de concorrência observado
                if current_count > max_concurrent:
                    max_concurrent = current_count

                active_nodes_list = [n for n, _ in active_entries]

                if current_count > 1:
                    # VIOLAÇÃO: mais de um processo simultâneo na SC
                    overlap_events += 1
                    print(
                        f"[VIOLACAO] Sobreposicao detectada! Ativos: {active_nodes_list} | req={req_id}",
                        flush=True
                    )
                else:
                    # Acesso exclusivo confirmado (comportamento esperado com sync ativo)
                    print(
                        f"[OBSERVER] BEGIN | node={node_id} | req={req_id} | ativos={current_count}",
                        flush=True
                    )

            elif event_type == "END":
                # Nó saiu da seção crítica: remove do conjunto de ativos
                active_entries.discard((node_id, req_id))
                total_completed += 1
                print(
                    f"[OBSERVER] END   | node={node_id} | req={req_id} | ativos={len(active_entries)}",
                    flush=True
                )

            # Registra no histórico circular (usado para diagnóstico e replay no Dashboard)
            if len(history) >= MAX_HISTORY:
                history.pop(0)
            history.append({
                "ts":     ts,
                "event":  event_type,
                "node":   node_id,
                "req_id": req_id,
                "active": len(active_entries)
            })

            is_overlap = (event_type == "BEGIN" and current_count > 1)
            active_cnt = len(active_entries)

        notify_dashboard({
            "type": "OBSERVER",
            "event": event_type,
            "node_id": node_id,
            "request_id": req_id,
            "overlap": is_overlap,
            "active_count": active_cnt
        })

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok"}).encode("utf-8"))

    def do_GET(self):
        if self.path == "/stats":
            # Retorna as métricas agregadas consultadas pelo collect_metrics.py
            # após cada rodada de testes para registrar o resultado do experimento.
            with lock:
                payload = {
                    "active_now":      [n for n, _ in active_entries],
                    "total_completed": total_completed,
                    "overlap_events":  overlap_events,
                    "max_concurrent":  max_concurrent
                }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload, indent=2).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()


if __name__ == "__main__":
    print("[OBSERVER] Iniciando na porta 9000...", flush=True)
    server = ThreadingHTTPServer(("0.0.0.0", 9000), ObserverHandler)
    server.serve_forever()

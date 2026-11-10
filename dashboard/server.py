"""
dashboard/server.py — Dashboard Web do Cluster Sync (Naimi–Tréhel)
BCC362 — Sistemas Distribuídos | UFOP | Professor Joubert Lima

Servidor FastAPI que recebe eventos HTTP dos nós e do observador e os transmite
via WebSocket ao navegador em tempo real. Exibe a topologia da árvore lógica
(Mermaid.js), linha do tempo de mensagens (Chart.js), métricas de Singhal e
heatmap de espera por nó. Suporta gravação e replay de sessões para apresentação.

ARQUITETURA (sem RabbitMQ):
  node.py / observer.py ──POST /event──► dashboard/server.py ──WebSocket /ws──► navegador

O dashboard é um serviço PASSIVO e OPCIONAL: não interfere no protocolo
Naimi–Tréhel, não concede nem bloqueia acessos, não altera os resultados
dos benchmarks automatizados (collect_metrics.py).
"""

import asyncio
import json
import os
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

app = FastAPI(title="Cluster Sync Dashboard")
app.mount("/static", StaticFiles(directory="dashboard/static"), name="static")

SESSIONS_DIR = "dashboard/sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Gerenciador de conexões WebSocket
# ---------------------------------------------------------------------------
class ConnectionManager:
    """Mantém a lista de browsers conectados e transmite eventos em broadcast."""

    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, payload: dict):
        """Transmite um dict JSON para todos os clientes conectados."""
        text = json.dumps(payload)
        for ws in list(self.active):
            try:
                await ws.send_text(text)
            except Exception:
                self.active.remove(ws)


manager = ConnectionManager()


# ---------------------------------------------------------------------------
# Gravação e replay de sessões (para a demonstração no seminário)
# ---------------------------------------------------------------------------
recording_buffer: list[dict] = []
is_recording: bool = False


def record_event(payload: dict):
    """Adiciona o evento ao buffer de gravação, se a gravação estiver ativa."""
    if is_recording:
        payload["_ts"] = datetime.now().isoformat()
        recording_buffer.append(payload)


# ---------------------------------------------------------------------------
# Endpoint principal: recebe eventos HTTP dos nós e do observer
# ---------------------------------------------------------------------------
@app.post("/event")
async def receive_event(request: Request):
    """
    Recebe eventos POST dos nós (node.py) e do observador (observer.py).

    Formato esperado (campos obrigatórios e opcionais):
    {
        "type":       "LOG" | "TOPOLOGY" | "METRICS" | "HEARTBEAT" | "OBSERVER",
        "node_id":    "node1",          # nó que originou o evento
        "message":    "...",            # texto livre para o painel de logs (LOG)
        "edges":      [{"from":"node1","to":"node2","token":true}, ...],  # TOPOLOGY
        "data":       {...},            # payload de métricas (METRICS)
        "request_id": "client1-001",   # identificador do pedido (OBSERVER)
        "event":      "BEGIN" | "END"  # para eventos do observer (OBSERVER)
    }

    O servidor normaliza o payload, o transmite via WebSocket e o grava
    no buffer de sessão se a gravação estiver ativa.
    """
    body = await request.json()
    event_type = body.get("type", "LOG")

    # Normaliza e enriquece o payload antes de retransmitir
    payload = {"type": event_type, **body}
    payload.setdefault("ts", datetime.now().isoformat())

    await manager.broadcast(payload)
    record_event(payload)
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# WebSocket: canal de streaming para o navegador
# ---------------------------------------------------------------------------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()   # mantém a conexão viva
    except WebSocketDisconnect:
        manager.disconnect(ws)


# ---------------------------------------------------------------------------
# Rota raiz: serve o index.html do dashboard
# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    with open("dashboard/static/index.html", encoding="utf-8") as f:
        return HTMLResponse(f.read())


# ---------------------------------------------------------------------------
# Endpoints de gravação e replay de sessões
# ---------------------------------------------------------------------------
@app.post("/replay/start-recording")
async def start_recording():
    global recording_buffer, is_recording
    recording_buffer = []
    is_recording = True
    return {"status": "Gravação iniciada."}


@app.post("/replay/stop-recording")
async def stop_recording():
    global is_recording
    is_recording = False
    session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"{SESSIONS_DIR}/{session_id}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(recording_buffer, f, indent=2, ensure_ascii=False)
    return {"status": f"Sessão salva: {session_id}", "events": len(recording_buffer)}


@app.get("/replay/sessions")
async def list_sessions():
    sessions = sorted(
        f.replace(".json", "")
        for f in os.listdir(SESSIONS_DIR)
        if f.endswith(".json")
    )
    return {"sessions": sessions}


@app.post("/replay/{session_id}")
async def replay_session(session_id: str):
    path = f"{SESSIONS_DIR}/{session_id}.json"
    if not os.path.exists(path):
        return {"status": "Sessão não encontrada."}

    with open(path, encoding="utf-8") as f:
        events = json.load(f)

    async def _replay():
        for event in events:
            await asyncio.sleep(0.3)
            await manager.broadcast(event)

    asyncio.create_task(_replay())
    return {"status": f"Reproduzindo {session_id} ({len(events)} eventos)."}


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)

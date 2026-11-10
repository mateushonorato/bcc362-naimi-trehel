"""
dashboard/server.py — Dashboard Web do Cluster Sync (Naimi–Tréhel)
BCC362 — Sistemas Distribuídos | UFOP | Professor Joubert Lima

Servidor FastAPI que recebe eventos HTTP dos nós e do observador e os transmite
via WebSocket ao navegador em tempo real. Mantém o estado consolidado em memória
para que conexões novas ou recarregamentos de página (F5) restaurem instantaneamente
a topologia, métricas e histórico completos.

ARQUITETURA (sem RabbitMQ):
  node.py / observer.py ──POST /event──► dashboard/server.py ──WebSocket /ws──► navegador
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
# Estado global consolidado mantido em memória
# ---------------------------------------------------------------------------
cluster_state = {
    "topology": {
        "edges": {"node2": "node1", "node3": "node1", "node4": "node1", "node5": "node1"},
        "token_holder": "node1",
        "in_cs": None,
        "known_nodes": ["node1", "node2", "node3", "node4", "node5"]
    },
    "metrics": {
        "total_messages": 0,
        "cs_entries": 0,
        "overlap_count": 0,
        "node_wait_times": {}
    },
    "recent_logs": [],
    "recent_timeline": []
}


def update_cluster_state(body: dict):
    event_type = body.get("type", "LOG")
    sender = body.get("node_id") or body.get("node")

    if sender and sender.startswith("node"):
        if sender not in cluster_state["topology"]["known_nodes"]:
            cluster_state["topology"]["known_nodes"].append(sender)
            cluster_state["topology"]["known_nodes"].sort()

    cluster_state["metrics"]["total_messages"] += 1

    if event_type == "TOPOLOGY":
        if body.get("edges"):
            for e in body["edges"]:
                f = e.get("from")
                t = e.get("to")
                if t is None or t == "":
                    cluster_state["topology"]["edges"].pop(f, None)
                else:
                    cluster_state["topology"]["edges"][f] = t
        if body.get("token_holder"):
            cluster_state["topology"]["token_holder"] = body["token_holder"]
        if "in_cs" in body:
            cluster_state["topology"]["in_cs"] = body["in_cs"]

    elif event_type == "OBSERVER":
        ev = body.get("event")
        if ev == "BEGIN":
            cluster_state["topology"]["in_cs"] = body.get("node_id")
        elif ev == "END":
            if cluster_state["topology"]["in_cs"] == body.get("node_id"):
                cluster_state["topology"]["in_cs"] = None
            cluster_state["metrics"]["cs_entries"] += 1
        if body.get("overlap"):
            cluster_state["metrics"]["overlap_count"] += 1

    elif event_type == "WAIT_TIME":
        w = body.get("wait_ms")
        if sender and w is not None:
            times = cluster_state["metrics"]["node_wait_times"].setdefault(sender, [])
            times.append(w)
            if len(times) > 50:
                times.pop(0)

    # Registra log recente
    msg = body.get("message")
    if msg:
        cluster_state["recent_logs"].insert(0, {
            "ts": datetime.now().strftime("%H:%M:%S"),
            "msg": msg,
            "sender": sender or "sistema"
        })
        if len(cluster_state["recent_logs"]) > 100:
            cluster_state["recent_logs"].pop()

        # Registra evento na timeline se for uma etapa chave do protocolo
        for kw in ["REQUEST", "TOKEN", "REV_PATH", "ENTER", "EXIT", "COMMITTED"]:
            if kw in msg:
                cluster_state["recent_timeline"].insert(0, {
                    "type": kw,
                    "desc": msg.replace(f"[{kw}]", "").replace(f"[{kw:<9}]", "").strip(),
                    "node": sender or "sistema",
                    "time": datetime.now().strftime("%H:%M:%S")
                })
                if len(cluster_state["recent_timeline"]) > 20:
                    cluster_state["recent_timeline"].pop()
                break


# ---------------------------------------------------------------------------
# Gerenciador de conexões WebSocket
# ---------------------------------------------------------------------------
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, payload: dict):
        text = json.dumps(payload)
        for ws in list(self.active):
            try:
                await ws.send_text(text)
            except Exception:
                self.active.remove(ws)


manager = ConnectionManager()

# ---------------------------------------------------------------------------
# Gravação e replay de sessões
# ---------------------------------------------------------------------------
recording_buffer: list[dict] = []
is_recording: bool = False


def record_event(payload: dict):
    if is_recording:
        payload["_ts"] = datetime.now().isoformat()
        recording_buffer.append(payload)


# ---------------------------------------------------------------------------
# Endpoints HTTP
# ---------------------------------------------------------------------------
@app.post("/event")
async def receive_event(request: Request):
    body = await request.json()
    event_type = body.get("type", "LOG")

    # Atualiza o estado central consolidado
    update_cluster_state(body)

    # Normaliza e transmite em broadcast
    payload = {"type": event_type, **body}
    payload.setdefault("ts", datetime.now().isoformat())

    await manager.broadcast(payload)
    record_event(payload)
    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    # Envia imediatamente o estado consolidado ao cliente recém-conectado
    await ws.send_text(json.dumps({
        "type": "INIT",
        "state": cluster_state
    }))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)


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
            update_cluster_state(event)
            await manager.broadcast(event)

    asyncio.create_task(_replay())
    return {"status": f"Reproduzindo {session_id} ({len(events)} eventos)."}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)

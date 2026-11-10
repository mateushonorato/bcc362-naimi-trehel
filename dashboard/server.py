"""
dashboard/server.py — Dashboard Web do Cluster Sync (Naimi–Tréhel)
BCC362 — Sistemas Distribuídos | UFOP | Professor Joubert Lima

Servidor FastAPI que recebe eventos HTTP dos nós e do observador e os transmite
via WebSocket ao navegador em tempo real. Mantém o estado consolidado em memória
para que conexões novas ou recarregamentos de página (F5) restaurem instantaneamente
a topologia, métricas e histórico completos.

Controles Avançados Integrados:
  - Visualização da Fila Distribuída FIFO (next_queue) e Árvore Lógica (father)
  - Player Multimídia Quadro a Quadro (Step-by-step) para apresentações
  - Gerenciador de Gravações e Replay (Upload, Download JSON, Dropdown)
  - Orquestrador de Carga e Cenários de Teste (Normal, Alta Contenção, Localidade, Passo Manual)
  - Botão de Reset de Métricas
"""

import asyncio
import json
import os
import random
import urllib.request
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse

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
        "known_nodes": ["node1", "node2", "node3", "node4", "node5"],
        "next_queues": {}
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

    elif event_type == "QUEUE":
        node = body.get("node")
        q = body.get("next_queue", [])
        if node:
            if "next_queues" not in cluster_state["topology"]:
                cluster_state["topology"]["next_queues"] = {}
            if q:
                cluster_state["topology"]["next_queues"][node] = q
            else:
                cluster_state["topology"]["next_queues"].pop(node, None)

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
recording_meta: dict = {}


def record_event(payload: dict):
    if is_recording:
        p = dict(payload)
        p["_ts"] = datetime.now().isoformat()
        recording_buffer.append(p)


# ---------------------------------------------------------------------------
# Orquestrador de Carga e Cenários de Teste (Opção 1 - Via HTTP nos Nós)
# ---------------------------------------------------------------------------
workload_tasks: list[asyncio.Task] = []
workload_status: dict = {
    "running": False,
    "scenario": None,
    "completed": 0,
    "total": 0,
    "message": "Nenhum teste em execução."
}


def _send_sync_request(url: str, body_bytes: bytes, timeout: float = 300.0):
    req = urllib.request.Request(
        url, data=body_bytes,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def resolve_node_url(node_id: str) -> str:
    override = os.environ.get(f"{node_id.upper()}_URL")
    if override:
        return override
    # Se estiver rodando dentro de rede Docker
    if os.environ.get("RUNNING_IN_DOCKER") or os.path.exists("/.dockerenv"):
        return f"http://{node_id}:8000"
    # Se estiver rodando no host diretamente com portas mapeadas
    if node_id.startswith("node"):
        try:
            num = int(node_id.replace("node", ""))
            return f"http://localhost:{8000 + num}"
        except ValueError:
            pass
    return f"http://{node_id}:8000"


async def run_node_workload(node_id: str, client_id: str, num_requests: int, min_sleep: float, max_sleep: float):
    target_url = resolve_node_url(node_id)
    for seq in range(1, num_requests + 1):
        if not workload_status["running"]:
            break
        req_id = f"{client_id}-{seq:03d}"
        body = json.dumps({"resource": "R", "request_id": req_id}).encode("utf-8")
        try:
            await asyncio.to_thread(_send_sync_request, f"{target_url}/client/request", body)
            workload_status["completed"] += 1
            await manager.broadcast({"type": "WORKLOAD_PROGRESS", "status": workload_status})
        except Exception:
            pass
        if seq < num_requests and max_sleep > 0 and workload_status["running"]:
            await asyncio.sleep(random.uniform(min_sleep, max_sleep))


# ---------------------------------------------------------------------------
# Endpoints HTTP da Aplicação
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
        "state": cluster_state,
        "workload": workload_status
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
# Endpoints de Controle de Testes (Workload)
# ---------------------------------------------------------------------------
@app.post("/api/workload/start")
async def start_workload(request: Request):
    global workload_tasks, workload_status
    if workload_status["running"]:
        return JSONResponse(status_code=400, content={"error": "Um teste já está em execução."})

    body = await request.json()
    scenario = body.get("scenario", "normal")
    num_reqs = int(body.get("num_requests", 5))

    workload_tasks.clear()
    workload_status["running"] = True
    workload_status["scenario"] = scenario
    workload_status["completed"] = 0

    if scenario == "step":
        target = body.get("node_id", "node1")
        workload_status["total"] = 1
        workload_status["message"] = f"Executando passo unitário: {target}"
        t = asyncio.create_task(run_node_workload(target, f"demo-{target}", 1, 0, 0))
        workload_tasks.append(t)

    elif scenario == "contention":
        nodes = ["node1", "node2", "node3", "node4", "node5"]
        workload_status["total"] = len(nodes) * num_reqs
        workload_status["message"] = f"Alta Contenção ({len(nodes)} nós, {num_reqs} reqs, sleep=0s)"
        for n in nodes:
            t = asyncio.create_task(run_node_workload(n, f"contention-{n}", num_reqs, 0.0, 0.1))
            workload_tasks.append(t)

    elif scenario == "skewed":
        nodes = ["node1", "node2", "node3", "node4", "node5"]
        workload_status["total"] = len(nodes) * num_reqs
        workload_status["message"] = "Carga Desigual (Localidade: nós 1 e 2 prioritários)"
        for n in nodes:
            if n in ["node1", "node2"]:
                t = asyncio.create_task(run_node_workload(n, f"skewed-{n}", num_reqs, 0.1, 0.3))
            else:
                t = asyncio.create_task(run_node_workload(n, f"skewed-{n}", num_reqs, 2.0, 4.0))
            workload_tasks.append(t)

    elif scenario == "ten-nodes":
        nodes = [f"node{i}" for i in range(1, 11)]
        workload_status["total"] = len(nodes) * num_reqs
        workload_status["message"] = f"Escala Ampliada (10 nós, {num_reqs} reqs)"
        for n in nodes:
            t = asyncio.create_task(run_node_workload(n, f"ten-{n}", num_reqs, 1.0, 3.0))
            workload_tasks.append(t)

    else:  # "normal"
        nodes = ["node1", "node2", "node3", "node4", "node5"]
        workload_status["total"] = len(nodes) * num_reqs
        workload_status["message"] = f"Carga Normal ({len(nodes)} nós, {num_reqs} reqs, sleep=1-5s)"
        for n in nodes:
            t = asyncio.create_task(run_node_workload(n, f"normal-{n}", num_reqs, 1.0, 5.0))
            workload_tasks.append(t)

    await manager.broadcast({"type": "WORKLOAD_START", "status": workload_status})

    async def _monitor_completion():
        try:
            await asyncio.gather(*workload_tasks)
        except asyncio.CancelledError:
            pass
        finally:
            workload_status["running"] = False
            workload_status["message"] = f"Teste '{scenario}' finalizado."
            await manager.broadcast({"type": "WORKLOAD_DONE", "status": workload_status})

    asyncio.create_task(_monitor_completion())
    return {"status": "started", "scenario": scenario, "total": workload_status["total"]}


@app.post("/api/workload/stop")
async def stop_workload():
    workload_status["running"] = False
    for t in workload_tasks:
        t.cancel()
    workload_tasks.clear()
    workload_status["message"] = "Teste abortado pelo usuário."
    await manager.broadcast({"type": "WORKLOAD_DONE", "status": workload_status})
    return {"status": "stopped"}


@app.get("/api/workload/status")
async def get_workload_status():
    return workload_status


@app.post("/api/reset")
async def reset_metrics():
    global cluster_state
    cluster_state["metrics"] = {
        "total_messages": 0,
        "cs_entries": 0,
        "overlap_count": 0,
        "node_wait_times": {}
    }
    cluster_state["recent_logs"] = []
    cluster_state["recent_timeline"] = []
    cluster_state["topology"]["next_queues"] = {}
    cluster_state["topology"]["in_cs"] = None
    await manager.broadcast({"type": "RESET", "state": cluster_state})
    return {"status": "ok", "state": cluster_state}


# ---------------------------------------------------------------------------
# Endpoints de Gravação, Exportação e Replay de Sessões
# ---------------------------------------------------------------------------
@app.post("/replay/start-recording")
async def start_recording(request: Request):
    global recording_buffer, is_recording, recording_meta
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    name = body.get("name") or f"Sessão {datetime.now().strftime('%d/%m %H:%M:%S')}"
    recording_meta = {
        "name": name,
        "started_at": datetime.now().isoformat(),
        "initial_state": json.loads(json.dumps(cluster_state))
    }
    recording_buffer = []
    is_recording = True
    return {"status": "Gravação iniciada.", "name": name}


@app.post("/replay/stop-recording")
async def stop_recording():
    global is_recording
    if not is_recording:
        return {"status": "Nenhuma gravação ativa."}
    is_recording = False
    session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_data = {
        "id": session_id,
        "name": recording_meta.get("name", session_id),
        "started_at": recording_meta.get("started_at"),
        "stopped_at": datetime.now().isoformat(),
        "events_count": len(recording_buffer),
        "initial_state": recording_meta.get("initial_state", {}),
        "events": recording_buffer
    }
    path = f"{SESSIONS_DIR}/{session_id}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(session_data, f, indent=2, ensure_ascii=False)
    return {
        "status": f"Sessão salva com sucesso!",
        "session_id": session_id,
        "name": session_data["name"],
        "events": len(recording_buffer)
    }


@app.get("/replay/sessions")
async def list_sessions():
    sessions = []
    for fname in sorted(os.listdir(SESSIONS_DIR), reverse=True):
        if fname.endswith(".json"):
            sid = fname.replace(".json", "")
            path = os.path.join(SESSIONS_DIR, fname)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict) and "events" in data:
                        sessions.append({
                            "id": sid,
                            "name": data.get("name", sid),
                            "date": data.get("started_at", ""),
                            "events_count": data.get("events_count", len(data["events"]))
                        })
                    elif isinstance(data, list):
                        sessions.append({
                            "id": sid,
                            "name": sid,
                            "date": "",
                            "events_count": len(data)
                        })
            except Exception:
                sessions.append({"id": sid, "name": sid, "date": "", "events_count": 0})
    return {"sessions": sessions}


@app.get("/replay/sessions/{session_id}")
async def get_session(session_id: str):
    path = os.path.join(SESSIONS_DIR, f"{session_id}.json")
    if not os.path.exists(path):
        return JSONResponse(status_code=404, content={"error": "Sessão não encontrada."})
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


@app.delete("/replay/sessions/{session_id}")
async def delete_session(session_id: str):
    path = os.path.join(SESSIONS_DIR, f"{session_id}.json")
    if os.path.exists(path):
        os.remove(path)
        return {"status": "Sessão removida."}
    return JSONResponse(status_code=404, content={"error": "Sessão não encontrada."})


@app.post("/replay/upload")
async def upload_session(request: Request):
    body = await request.json()
    session_id = body.get("id") or datetime.now().strftime("%Y%m%d_%H%M%S_import")
    path = os.path.join(SESSIONS_DIR, f"{session_id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(body, f, indent=2, ensure_ascii=False)
    return {"status": "ok", "id": session_id}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)

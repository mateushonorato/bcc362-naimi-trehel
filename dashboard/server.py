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
import socket
import subprocess
import urllib.request
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="Cluster Sync Dashboard")
app.mount("/static", StaticFiles(directory="dashboard/static"), name="static")

SESSIONS_DIR = "dashboard/sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Escala ativa do cluster (5 ou 10 nós)
# ---------------------------------------------------------------------------
current_scale = 5


def get_host_project_dir() -> str:
    """Detecta dinamicamente o diretório do projeto no host (sem caminhos hardcoded)."""
    # 1. Se explicitamente configurado pelo usuário, respeita
    env_dir = os.environ.get("HOST_PROJECT_DIR")
    if env_dir:
        return env_dir

    # 2. Se estiver rodando dentro do container Docker com docker.sock montado,
    # consulta o Docker daemon para descobrir onde o diretório /app está montado no Host.
    if os.environ.get("RUNNING_IN_DOCKER"):
        hostname = os.environ.get("HOSTNAME", socket.gethostname())
        try:
            cmd = f'docker inspect {hostname} --format "{{{{ range .Mounts }}}}{{{{ if eq .Destination \\"/app\\" }}}}{{{{ .Source }}}}{{{{ end }}}}{{{{ end }}}}"'
            res = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=5)
            if res.returncode == 0 and res.stdout.strip():
                return res.stdout.strip()

            cmd_label = f'docker inspect {hostname} --format "{{{{ index .Config.Labels \\"com.docker.compose.project.working_dir\\" }}}}"'
            res_label = subprocess.run(cmd_label, shell=True, capture_output=True, text=True, timeout=5)
            if res_label.returncode == 0 and res_label.stdout.strip():
                return res_label.stdout.strip()
        except Exception as e:
            print(f"[WARN] Falha ao autodetectar HOST_PROJECT_DIR via docker inspect: {e}", flush=True)

    # 3. Fallback: diretório raiz do projeto no filesystem (execução fora do Docker)
    return str(Path(__file__).resolve().parent.parent)


def get_docker_project_name() -> str:
    """Detecta o nome do projeto Docker Compose dos nós existentes (ex: sd2)."""
    try:
        cmd = 'docker ps -a --filter "name=node1" --format "{{.Label \\"com.docker.compose.project\\"}}"'
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=5)
        lines = [line.strip() for line in res.stdout.splitlines() if line.strip()]
        if lines:
            return lines[0]
    except Exception:
        pass
    return ""


def set_cluster_scale_containers(scale: int):
    """Sobe ou para contêineres node6..node10 via Docker Compose dinamicamente."""
    host_project_dir = get_host_project_dir()
    compose_f1 = "/app/docker-compose.yml" if os.path.exists("/app/docker-compose.yml") else "docker-compose.yml"
    compose_f2 = "/app/docker-compose.ten-nodes.yml" if os.path.exists("/app/docker-compose.ten-nodes.yml") else "docker-compose.ten-nodes.yml"

    proj_dir_arg = f"--project-directory {host_project_dir}" if host_project_dir else ""
    proj_name = get_docker_project_name()
    proj_name_arg = f"-p {proj_name}" if proj_name else ""

    if scale == 10:
        cmd = f"docker compose {proj_name_arg} -f {compose_f1} -f {compose_f2} {proj_dir_arg} up -d node6 node7 node8 node9 node10"
    else:
        cmd = f"docker compose {proj_name_arg} -f {compose_f1} -f {compose_f2} {proj_dir_arg} stop -t 1 node6 node7 node8 node9 node10"

    print(f"[SCALE] Executing: {cmd}", flush=True)
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"[SCALE ERROR] {res.stderr}", flush=True)
    else:
        print(f"[SCALE SUCCESS] Cluster scale set to {scale} nodes.", flush=True)


def sync_cluster_topology_scale(scale: int):
    """Sincroniza os nós conhecidos e arestas no estado do dashboard."""
    global cluster_state
    if scale == 10:
        cluster_state["topology"]["known_nodes"] = [f"node{i}" for i in range(1, 11)]
        for i in range(6, 11):
            n_id = f"node{i}"
            if n_id not in cluster_state["topology"]["edges"]:
                cluster_state["topology"]["edges"][n_id] = "node1"
    else:
        cluster_state["topology"]["known_nodes"] = [f"node{i}" for i in range(1, 6)]
        for i in range(6, 11):
            n_id = f"node{i}"
            cluster_state["topology"]["edges"].pop(n_id, None)
            cluster_state["topology"]["next_queues"].pop(n_id, None)
            cluster_state["metrics"]["node_wait_times"].pop(n_id, None)

        # Se algum nó remanescente apontava para nós desligados, reconecta à raiz
        for i in range(1, 6):
            n_id = f"node{i}"
            f = cluster_state["topology"]["edges"].get(n_id)
            if f and f in [f"node{k}" for k in range(6, 11)]:
                cluster_state["topology"]["edges"][n_id] = "node1" if n_id != "node1" else None

        if cluster_state["topology"]["token_holder"] in [f"node{k}" for k in range(6, 11)]:
            cluster_state["topology"]["token_holder"] = "node1"

    # Reseta os nós e o observer para estado consistente
    reset_nodes_and_observer(scale)


def reset_nodes_and_observer(scale: int):
    """Envia requisição /reset aos nós ativos e ao observador passivo."""
    nodes = [f"node{i}" for i in range(1, scale + 1)]
    for node_id in nodes:
        url = resolve_node_url(node_id)
        try:
            _send_sync_request(f"{url}/reset", b"{}", timeout=2.0)
        except Exception:
            pass
    obs_url = os.environ.get("OBSERVER_URL", "http://observer:9000")
    try:
        _send_sync_request(f"{obs_url}/reset", b"{}", timeout=2.0)
    except Exception:
        pass


def set_nodes_sync_enabled(nodes: list, enabled: bool):
    """Habilita ou desabilita o algoritmo de sincronização nos nós (Prova de Instrumentação)."""
    for node_id in nodes:
        url = resolve_node_url(node_id)
        try:
            body = json.dumps({"enabled": enabled}).encode("utf-8")
            _send_sync_request(f"{url}/sync/set", body, timeout=2.0)
        except Exception as e:
            print(f"[WARN] Failed to set sync on {node_id}: {e}", flush=True)

# ---------------------------------------------------------------------------
# Estado global consolidado mantido em memória
# ---------------------------------------------------------------------------
cluster_state = {
    "topology": {
        "edges": {"node2": "node1", "node3": "node1", "node4": "node1", "node5": "node1"},
        "token_holder": "node1",
        "in_cs": [],
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
        try:
            num = int(sender.replace("node", ""))
            if num <= current_scale and sender not in cluster_state["topology"]["known_nodes"]:
                cluster_state["topology"]["known_nodes"].append(sender)
                cluster_state["topology"]["known_nodes"].sort(key=lambda x: int(x.replace("node", "")))
        except ValueError:
            pass

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
            in_cs_val = body["in_cs"]
            if not isinstance(cluster_state["topology"].get("in_cs"), list):
                cluster_state["topology"]["in_cs"] = []
            if isinstance(in_cs_val, list):
                cluster_state["topology"]["in_cs"] = list(in_cs_val)
            elif in_cs_val is None:
                node_to_remove = body.get("node_id") or sender
                if node_to_remove and node_to_remove in cluster_state["topology"]["in_cs"]:
                    cluster_state["topology"]["in_cs"].remove(node_to_remove)
            elif isinstance(in_cs_val, str) and in_cs_val:
                if in_cs_val not in cluster_state["topology"]["in_cs"]:
                    cluster_state["topology"]["in_cs"].append(in_cs_val)
            body["in_cs"] = list(cluster_state["topology"]["in_cs"])

    elif event_type == "QUEUE":
        node = body.get("node") or body.get("node_id")
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
        nid = body.get("node_id")
        active_nodes = body.get("active_nodes")

        if not isinstance(cluster_state["topology"].get("in_cs"), list):
            old = cluster_state["topology"].get("in_cs")
            cluster_state["topology"]["in_cs"] = [old] if old else []

        if active_nodes is not None:
            cluster_state["topology"]["in_cs"] = list(active_nodes)
        else:
            if ev == "BEGIN" and nid:
                if nid not in cluster_state["topology"]["in_cs"]:
                    cluster_state["topology"]["in_cs"].append(nid)
            elif ev == "END" and nid:
                if nid in cluster_state["topology"]["in_cs"]:
                    cluster_state["topology"]["in_cs"].remove(nid)

        body["in_cs"] = list(cluster_state["topology"]["in_cs"])

        if ev == "END":
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
            "ts": datetime.now().astimezone().isoformat(),
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
                    "time": datetime.now().astimezone().isoformat()
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
        p["_ts"] = datetime.now().astimezone().isoformat()
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
    payload.setdefault("ts", datetime.now().astimezone().isoformat())

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
        "workload": workload_status,
        "scale": current_scale
    }))
    try:
        while True:
            data = await ws.receive_text()
            if data == "GET_STATE":
                await ws.send_text(json.dumps({
                    "type": "INIT",
                    "state": cluster_state,
                    "workload": workload_status,
                    "scale": current_scale
                }))
    except WebSocketDisconnect:
        manager.disconnect(ws)


@app.get("/")
async def root():
    with open("dashboard/static/index.html", encoding="utf-8") as f:
        return HTMLResponse(f.read())


# ---------------------------------------------------------------------------
# Endpoints de Topologia e Escala Dinâmica (5 ou 10 Nós)
# ---------------------------------------------------------------------------
@app.post("/api/cluster/scale")
async def change_scale(request: Request):
    global current_scale
    body = await request.json()
    scale = int(body.get("scale", 5))
    if scale not in [5, 10]:
        return JSONResponse(status_code=400, content={"error": "Escala inválida (use 5 ou 10)"})

    current_scale = scale
    await asyncio.to_thread(set_cluster_scale_containers, scale)
    sync_cluster_topology_scale(scale)

    await manager.broadcast({
        "type": "SCALE_CHANGED",
        "scale": scale,
        "state": cluster_state
    })
    return {"status": "ok", "scale": scale, "state": cluster_state}


# ---------------------------------------------------------------------------
# Endpoints de Controle de Testes (Workload)
# ---------------------------------------------------------------------------
@app.post("/api/workload/start")
async def start_workload(request: Request):
    global workload_tasks, workload_status, current_scale
    if workload_status["running"]:
        return JSONResponse(status_code=400, content={"error": "Um teste já está em execução."})

    body = await request.json()
    scenario = body.get("scenario", "normal")
    scale = int(body.get("scale", current_scale))
    num_reqs = int(body.get("num_requests", 5))

    # Sincroniza a escala se o usuário alterou
    if scale != current_scale:
        current_scale = scale
        await asyncio.to_thread(set_cluster_scale_containers, scale)
        sync_cluster_topology_scale(scale)

    nodes = [f"node{i}" for i in range(1, scale + 1)]

    workload_tasks.clear()
    workload_status["running"] = True
    workload_status["scenario"] = scenario
    workload_status["completed"] = 0

    if scenario == "step":
        targets = body.get("nodes") or [body.get("node_id", "node1")]
        workload_status["total"] = len(targets)
        workload_status["message"] = f"Executando passo: {', '.join(targets)}"
        for target in targets:
            t = asyncio.create_task(run_node_workload(target, f"demo-{target}", 1, 0, 0))
            workload_tasks.append(t)

    elif scenario == "disabled":
        # Prova de Instrumentação: Desativa a sincronização distribuída nos nós
        await asyncio.to_thread(set_nodes_sync_enabled, nodes, False)
        workload_status["total"] = len(nodes) * num_reqs
        workload_status["message"] = f"⚠️ Prova de Instrumentação (Sync OFF, {len(nodes)} nós, {num_reqs} reqs/nó)"
        for n in nodes:
            t = asyncio.create_task(run_node_workload(n, f"disabled-{n}", num_reqs, 0.0, 0.0))
            workload_tasks.append(t)

    elif scenario == "contention":
        workload_status["total"] = len(nodes) * num_reqs
        workload_status["message"] = f"Alta Contenção ({len(nodes)} nós, {num_reqs} reqs/nó, sleep=0s)"
        for n in nodes:
            t = asyncio.create_task(run_node_workload(n, f"contention-{n}", num_reqs, 0.0, 0.05))
            workload_tasks.append(t)

    elif scenario == "skewed":
        workload_status["total"] = len(nodes) * num_reqs
        workload_status["message"] = f"Carga Desigual / Localidade ({len(nodes)} nós, nós 1 e 2 prioritários)"
        for n in nodes:
            if n in ["node1", "node2"]:
                t = asyncio.create_task(run_node_workload(n, f"skewed-{n}", num_reqs, 0.1, 0.3))
            else:
                t = asyncio.create_task(run_node_workload(n, f"skewed-{n}", num_reqs, 2.0, 4.0))
            workload_tasks.append(t)

    else:  # "normal"
        workload_status["total"] = len(nodes) * num_reqs
        workload_status["message"] = f"Carga Normal ({len(nodes)} nós, {num_reqs} reqs/nó, sleep=1–5s)"
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
            if scenario == "disabled":
                # Sempre restaura a sincronização distribuída ao término da prova
                await asyncio.to_thread(set_nodes_sync_enabled, nodes, True)
            workload_status["running"] = False
            workload_status["message"] = f"Teste '{scenario}' finalizado."
            await manager.broadcast({"type": "WORKLOAD_DONE", "status": workload_status})

    asyncio.create_task(_monitor_completion())
    return {"status": "started", "scenario": scenario, "scale": scale, "total": workload_status["total"]}


@app.post("/api/workload/stop")
async def stop_workload():
    workload_status["running"] = False
    for t in workload_tasks:
        t.cancel()
    workload_tasks.clear()
    # Garante restauração da sincronização caso o teste estivesse em modo disabled
    nodes = [f"node{i}" for i in range(1, current_scale + 1)]
    await asyncio.to_thread(set_nodes_sync_enabled, nodes, True)
    workload_status["message"] = "Teste abortado pelo usuário."
    await manager.broadcast({"type": "WORKLOAD_DONE", "status": workload_status})
    return {"status": "stopped"}


@app.get("/api/workload/status")
async def get_workload_status():
    return workload_status


@app.post("/api/reset")
async def reset_metrics():
    global cluster_state
    # Restaura sincronização e estado inicial nos nós ativos
    active_nodes = [f"node{i}" for i in range(1, current_scale + 1)]
    await asyncio.to_thread(set_nodes_sync_enabled, active_nodes, True)
    await asyncio.to_thread(reset_nodes_and_observer, current_scale)

    cluster_state["metrics"] = {
        "total_messages": 0,
        "cs_entries": 0,
        "overlap_count": 0,
        "node_wait_times": {}
    }
    cluster_state["recent_logs"] = []
    cluster_state["recent_timeline"] = []
    cluster_state["topology"]["next_queues"] = {}
    cluster_state["topology"]["in_cs"] = []
    cluster_state["topology"]["token_holder"] = "node1"
    cluster_state["topology"]["edges"] = {
        f"node{i}": "node1" for i in range(2, current_scale + 1)
    }
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
        "started_at": datetime.now().astimezone().isoformat(),
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
        "stopped_at": datetime.now().astimezone().isoformat(),
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

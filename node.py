"""
node.py — Nó do Cluster Sync (Algoritmo de Naimi–Tréhel, 1996)
BCC362 — Sistemas Distribuídos | UFOP | Professor Joubert Lima

Implementa exclusão mútua distribuída via token e árvore lógica dinâmica com
reversão de caminhos (path reversal). Cada instância deste script representa um
processo independente no Cluster Sync, executado em seu próprio contêiner Docker.

Referência: Naimi, M., Tréhel, M., & Arnold, A. (1996). A Log(N) Distributed
Mutual Exclusion Algorithm Based on Path Reversal. Journal of Parallel and
Distributed Computing, 34(1), 1–13. https://doi.org/10.1006/jpdc.1996.0041
"""

import json
import os
import queue
import random
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------------------------------------------------------------------------
# Configuração via variáveis de ambiente (injetadas pelo Docker Compose/Swarm)
# ---------------------------------------------------------------------------
NODE_ID       = os.environ.get("NODE_ID", "node1")
PORT          = int(os.environ.get("PORT", "8000"))

# INITIAL_FATHER: ponteiro inicial na árvore lógica.
#   - node1 (raiz inicial e portador do token): INITIAL_FATHER = "" → None
#   - todos os demais nós: INITIAL_FATHER = "node1" (apontam para a raiz)
INITIAL_FATHER = os.environ.get("INITIAL_FATHER", "") or None

# HAS_TOKEN: apenas node1 inicia com o token (True); os demais partem sem ele.
HAS_TOKEN_INIT = os.environ.get("HAS_TOKEN", "false").lower() == "true"

OBSERVER_URL  = os.environ.get("OBSERVER_URL", "http://observer:9000")
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "http://dashboard:8080")

# SYNC_ENABLED: quando False, desativa o protocolo distribuído para o experimento
# de prova de instrumentação (disabled_sync), permitindo sobreposições propositais.
SYNC_ENABLED  = os.environ.get("SYNC_ENABLED", "true").lower() == "true"

# Duração aleatória da seção crítica simulada (sleep entre BEGIN e END).
# Conforme enunciado: 0,2 s a 1,0 s.
CS_MIN_SLEEP  = float(os.environ.get("CS_MIN_SLEEP", "0.2"))
CS_MAX_SLEEP  = float(os.environ.get("CS_MAX_SLEEP", "1.0"))

# Mapa de peers: nome DNS do serviço Docker → URL base HTTP.
# Pré-computado para evitar concatenação de strings em tempo crítico.
PEERS = {
    f"node{i}": f"http://node{i}:8000" for i in range(1, 11)
}


# ---------------------------------------------------------------------------
# Classe principal: implementa o estado e o protocolo de Naimi–Tréhel
# ---------------------------------------------------------------------------
class NaimiTrehelNode:
    """
    Estado local do nó e lógica do algoritmo de Naimi–Tréhel.

    Cada nó mantém quatro variáveis de estado fundamentais (Seção 2 do paper):
      - father:     ponteiro para o nó pai na árvore lógica dinâmica.
                    None indica que este nó é a raiz atual (portador ou solicitante).
      - next_queue: fila FIFO de nós que aguardam o token após a saída da SC.
      - requesting: True enquanto este nó aguarda ou está na seção crítica.
      - has_token:  True quando este nó detém a autorização exclusiva de acesso.

    A invariante central do algoritmo: existe exatamente UM token no sistema
    em qualquer instante, e o token só avança quando há um solicitante em fila.
    """

    def __init__(self):
        # Mutex local para atomicidade na manipulação do estado do nó.
        # Protege father, next_queue, requesting e has_token contra condições de corrida.
        # IMPORTANTE: não substitui o protocolo distribuído — apenas protege a memória local.
        self.state_lock = threading.Lock()

        # Mutex local para serializar pedidos de clientes concorrentes no mesmo nó.
        # Garante que, se dois clientes chamarem /client/request simultaneamente,
        # apenas um entrará no protocolo distribuído por vez.
        self.client_entry_lock = threading.Lock()

        # Estado inicial da árvore lógica (definido via variável de ambiente)
        self.father     = INITIAL_FATHER
        self.next_queue = []
        self.requesting = False
        self.has_token  = HAS_TOKEN_INIT

        # Evento de sincronização intra-processo: o acquire() bloqueia neste evento
        # e o on_receive_token() o sinaliza quando o token chega.
        self.token_event = threading.Event()
        if self.has_token:
            self.token_event.set()  # node1 não precisa esperar: já tem o token

        # Contadores de telemetria para coleta de métricas
        self.protocol_msgs_sent = 0  # Total de mensagens REQUEST + TOKEN enviadas
        self.acquisitions       = 0  # Número de entradas na seção crítica
        self.wait_times_ms      = [] # Histórico de tempos de espera (em ms, monotônico)

        # -----------------------------------------------------------------------
        # Canais FIFO por destinatário (garantia fundamental do Naimi–Tréhel)
        # -----------------------------------------------------------------------
        # O algoritmo exige que as mensagens entre quaisquer dois nós cheguem
        # na ordem em que foram enviadas (canais FIFO). Implementamos isso com
        # uma queue.Queue e uma thread worker dedicada por peer: a thread retira
        # mensagens da fila e as envia via HTTP em sequência, sem interleaving.
        self.send_queues = {}
        for peer_id in PEERS:
            if peer_id != NODE_ID:
                q = queue.Queue()
                self.send_queues[peer_id] = q
                t = threading.Thread(
                    target=self._sender_worker, args=(peer_id, q), daemon=True
                )
                t.start()

        # Fila assíncrona não-bloqueante para envio de eventos ao Dashboard Web
        self.dashboard_queue = queue.Queue(maxsize=1000)
        if DASHBOARD_URL:
            t = threading.Thread(target=self._dashboard_worker, daemon=True)
            t.start()
            # Notifica topologia inicial ao Dashboard Web
            if self.father:
                self.notify_dashboard("TOPOLOGY", {"edges": [{"from": NODE_ID, "to": self.father}]})
            if self.has_token:
                self.notify_dashboard("TOPOLOGY", {"token_holder": NODE_ID})
            self.notify_dashboard("QUEUE", {"node": NODE_ID, "next_queue": []})

    def _dashboard_worker(self):
        """Worker em background que envia telemetria ao Dashboard sem bloquear o protocolo."""
        while True:
            payload = self.dashboard_queue.get()
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
            finally:
                self.dashboard_queue.task_done()

    def notify_dashboard(self, event_type, payload):
        """Enfileira evento para o Dashboard Web (não bloqueante, O(1), descarta se cheio)."""
        if not DASHBOARD_URL:
            return
        item = {"type": event_type, "node_id": NODE_ID, **payload}
        try:
            self.dashboard_queue.put_nowait(item)
        except queue.Full:
            pass

    # -----------------------------------------------------------------------
    # Envio de mensagens de protocolo
    # -----------------------------------------------------------------------

    def _sender_worker(self, target_id, q):
        """Thread dedicada ao envio FIFO de mensagens para um único peer.

        Retira requisições da fila e as envia via HTTP POST, retentando
        indefinidamente em caso de falha transitória de rede (compatível com
        o pressuposto de canal confiável do enunciado do TP2).
        """
        while True:
            endpoint, payload = q.get()
            url  = f"{PEERS[target_id]}{endpoint}"
            body = json.dumps(payload).encode("utf-8")
            while True:  # retenta até a mensagem ser entregue
                try:
                    req = urllib.request.Request(
                        url, data=body,
                        headers={"Content-Type": "application/json"},
                        method="POST"
                    )
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        resp.read()
                    break
                except Exception:
                    time.sleep(0.05)
            q.task_done()

    def send_async(self, target_id, endpoint, payload):
        """Enfileira uma mensagem de protocolo para o peer target_id.

        O envio ocorre de forma assíncrona pela thread worker dedicada,
        garantindo a ordenação FIFO sem bloquear o processamento atual.
        Incrementa o contador de mensagens de protocolo para telemetria.
        """
        self.protocol_msgs_sent += 1
        self.send_queues[target_id].put((endpoint, payload))

    # -----------------------------------------------------------------------
    # Instrumentação e logs
    # -----------------------------------------------------------------------

    def notify_observer(self, event_type, request_id):
        """Envia notificação BEGIN ou END ao observador passivo.

        O observador usa esses eventos para detectar sobreposições de acesso
        à seção crítica sem precisar comparar relógios físicos de máquinas distintas.
        A notificação é feita com retentativas para tolerar atrasos no startup.
        """
        body = json.dumps({
            "event":      event_type,
            "node_id":    NODE_ID,
            "request_id": request_id
        }).encode("utf-8")
        url = f"{OBSERVER_URL}/event"
        for _ in range(30):
            try:
                req = urllib.request.Request(
                    url, data=body,
                    headers={"Content-Type": "application/json"},
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    resp.read()
                return
            except Exception:
                time.sleep(0.5)
        print(f"[WARN] Falha ao notificar observer ({event_type}) para {request_id}", flush=True)

    def log_event(self, stage, request_id, extra=""):
        """Registra um evento no stdout com o estado completo do nó.

        Formato padronizado para facilitar correlação de logs entre contêineres:
        [STAGE    ] node=<id> | req=<request_id> | token=<bool> | father=<id> | next=[...] <extra>
        """
        print(
            f"[{stage:<9}] node={NODE_ID} | req={request_id} | token={self.has_token} "
            f"| father={self.father} | next={self.next_queue} {extra}",
            flush=True
        )

    # -----------------------------------------------------------------------
    # Primitivas do protocolo: acquire e release
    # -----------------------------------------------------------------------

    def acquire(self, resource, request_id):
        """Solicita autorização exclusiva para acessar o recurso R.

        Implementa a fase de REQUEST do algoritmo de Naimi–Tréhel:

        1. Se father != None: este nó NÃO é a raiz da árvore.
           - Salva o father atual e define father = None (torna-se a nova raiz).
           - Envia REQUEST(requester=self) ao antigo father, que propagará
             pela árvore até chegar ao portador do token.
           - Bloqueia em token_event.wait() até receber o TOKEN.

        2. Se father == None: este nó JÁ É a raiz (portador do token).
           - Sinaliza token_event diretamente: acesso imediato sem mensagens.
           - Este é o "efeito de localidade": 0 mensagens trafegadas pela rede.

        Retorna o tempo de espera em milissegundos (medido com relógio
        monotônico para evitar comparação de clocks entre hosts distintos).
        """
        start_mono = time.monotonic()

        # Modo de prova de instrumentação: sem protocolo, sem espera.
        if not SYNC_ENABLED:
            self.log_event("REQUEST", request_id, "(SYNC_DISABLED)")
            self.log_event("ACQUIRE", request_id, "(SYNC_DISABLED)")
            return (time.monotonic() - start_mono) * 1000.0

        with self.state_lock:
            self.requesting = True
            self.log_event("REQUEST", request_id)

            if self.father is not None:
                # Caso geral: propaga o pedido pela árvore em direção ao token.
                # Redefinir father=None ANTES do envio marca este nó como nova raiz,
                # garantindo que futuros REQUESTs recebidos sejam corretamente enfileirados.
                target = self.father
                self.father = None
                self.token_event.clear()
                self.notify_dashboard("TOPOLOGY", {"edges": [{"from": NODE_ID, "to": None}]})
                self.notify_dashboard("LOG", {"message": f"[{NODE_ID}] REQUEST {request_id} -> enviado para {target}"})
                self.send_async(
                    target, "/protocol/request",
                    {"requester": NODE_ID, "request_id": request_id}
                )
            else:
                # Este nó já é a raiz (tem o token): acesso imediato.
                self.token_event.set()
                self.notify_dashboard("LOG", {"message": f"[{NODE_ID}] REQUEST {request_id} -> posse local do token (0 msgs)"})

        # Aguarda a chegada do TOKEN (ou o set() imediato acima).
        self.token_event.wait()
        wait_ms = (time.monotonic() - start_mono) * 1000.0

        with self.state_lock:
            self.acquisitions += 1
            self.wait_times_ms.append(wait_ms)
            self.log_event("ACQUIRE", request_id, f"| wait_ms={wait_ms:.2f}")
        return wait_ms

    def release(self, resource, request_id):
        """Libera o token ao término da seção crítica.

        Implementa a fase de RELEASE do algoritmo de Naimi–Tréhel:

        - Se next_queue não está vazia: existe um nó aguardando.
          Retira o primeiro da fila (FIFO) e envia TOKEN diretamente a ele.
          Este é o mecanismo de justiça: nenhum nó enfileirado é ultrapassado.

        - Se next_queue está vazia: nenhum outro nó está esperando.
          O token permanece neste nó (has_token continua True) até que um
          REQUEST chegue, momento em que on_receive_request() o encaminha.
        """
        if not SYNC_ENABLED:
            self.log_event("RELEASE", request_id, "(SYNC_DISABLED)")
            return

        with self.state_lock:
            self.requesting = False

            if len(self.next_queue) > 0:
                # Encaminha o token ao próximo solicitante enfileirado (FIFO)
                next_node = self.next_queue.pop(0)
                self.has_token = False
                self.token_event.clear()
                self.notify_dashboard("QUEUE", {"node": NODE_ID, "next_queue": list(self.next_queue)})
                self.notify_dashboard("TOPOLOGY", {"token_holder": next_node})
                self.notify_dashboard("LOG", {"message": f"[{NODE_ID}] RELEASE -> token enviado para {next_node}"})
                self.log_event("RELEASE", request_id, f"-> enviando token para {next_node}")
                self.send_async(
                    next_node, "/protocol/token",
                    {"from": NODE_ID, "request_id": request_id}
                )
            else:
                # Nenhum solicitante pendente: retém o token localmente
                self.notify_dashboard("LOG", {"message": f"[{NODE_ID}] RELEASE -> mantendo token local"})
                self.log_event("RELEASE", request_id, "-> mantendo token local")

    # -----------------------------------------------------------------------
    # Handlers de mensagens de protocolo (recebidas via HTTP)
    # -----------------------------------------------------------------------

    def on_receive_request(self, requester, request_id):
        """Processa uma mensagem REQUEST recebida de outro nó.

        Esta é a etapa central da reversão de caminhos (path reversal):

        Ao receber REQUEST(requester=X):
          1. Se father == None: este nó é a raiz (portador ou solicitante atual).
             a. Se requesting: já está usando ou aguardando o token → enfileira X.
             b. Se não requesting: tem o token livre → envia TOKEN diretamente a X.

          2. Se father != None: este nó é intermediário na árvore.
             → Encaminha REQUEST(requester=X) ao seu father atual (propaga em direção ao token).

          Em ambos os casos: redefine father = requester (REVERSÃO DE CAMINHOS).
          Isso faz o ponteiro apontar diretamente para o solicitante original,
          achata a árvore e reduz o custo amortizado para O(log N).

        DETALHE CRÍTICO: o parâmetro 'requester' é o ID do nó que ORIGINOU o pedido,
        não o nó que intermediou o envio desta mensagem. Isso é o que garante que
        o father aponte sempre para o solicitante original e não para vizinhos.
        """
        with self.state_lock:
            prev_father = self.father

            if self.father is None:
                if self.requesting:
                    # Já ocupa ou aguarda a SC: enfileira o solicitante
                    self.next_queue.append(requester)
                    self.notify_dashboard("QUEUE", {"node": NODE_ID, "next_queue": list(self.next_queue)})
                else:
                    # Token livre: entrega diretamente ao solicitante
                    self.has_token = False
                    self.token_event.clear()
                    self.send_async(
                        requester, "/protocol/token",
                        {"from": NODE_ID, "request_id": request_id}
                    )
            else:
                # Nó intermediário: encaminha o REQUEST na direção do token
                self.send_async(
                    self.father, "/protocol/request",
                    {"requester": requester, "request_id": request_id}
                )

            # REVERSÃO DE CAMINHOS: redireciona father para o solicitante original
            self.father = requester
            self.notify_dashboard("TOPOLOGY", {"edges": [{"from": NODE_ID, "to": requester}]})
            self.notify_dashboard("LOG", {"message": f"[REV_PATH ] node={NODE_ID} | father: {prev_father} -> {self.father}"})
            print(
                f"[REV_PATH ] node={NODE_ID} | req_from={requester} "
                f"| father: {prev_father} -> {self.father} | next={self.next_queue}",
                flush=True
            )

    def on_receive_token(self, sender, request_id):
        """Processa a chegada do TOKEN enviado por outro nó.

        Sinaliza token_event para desbloquear o acquire() que está em espera.
        A partir deste momento, o nó detém a autorização exclusiva de acesso à SC.
        """
        with self.state_lock:
            self.has_token = True
            self.token_event.set()
            self.notify_dashboard("TOPOLOGY", {"token_holder": NODE_ID})
            self.notify_dashboard("LOG", {"message": f"[{NODE_ID}] TOKEN recebido de {sender}"})
            self.log_event("TOKEN_RECV", request_id, f"<- recebido de {sender}")

    # -----------------------------------------------------------------------
    # Ponto de entrada para requisições dos clientes
    # -----------------------------------------------------------------------

    def handle_client_write(self, resource, request_id):
        """Coordena o ciclo completo de acesso à seção crítica para um cliente.

        Fluxo de execução (mapeado diretamente nos logs do experimento):
          1. [REQUEST ] acquire() inicia o protocolo distribuído.
          2. [ACQUIRE ] token_event sinalizado: nó entrou na SC.
          3. [ENTER   ] Notifica BEGIN ao observador.
          4.             Simula acesso a R com sleep aleatório (0,2 a 1,0 s).
          5. [EXIT    ] Notifica END ao observador.
          6. [RELEASE ] release() encaminha o token ao próximo solicitante.
          7. [COMMITTED] Retorna confirmação ao cliente com o tempo de espera.

        O bloco try/finally garante que release() seja chamado mesmo em caso
        de exceção durante a seção crítica (ausência de deadlock por abandono).

        client_entry_lock serializa múltiplos clientes que possam contatar este
        nó simultaneamente, evitando que dois acquire() concorram no mesmo processo.
        """
        with self.client_entry_lock:
            wait_ms = self.acquire(resource, request_id)
            try:
                self.log_event("ENTER", request_id)
                self.notify_observer("BEGIN", request_id)
                self.notify_dashboard("TOPOLOGY", {"in_cs": NODE_ID})
                self.notify_dashboard("LOG", {"message": f"[{NODE_ID}] ENTER seção crítica ({request_id})"})
                time.sleep(random.uniform(CS_MIN_SLEEP, CS_MAX_SLEEP))  # simula acesso a R
                self.notify_observer("END", request_id)
                self.log_event("EXIT", request_id)
                self.notify_dashboard("TOPOLOGY", {"in_cs": None})
                self.notify_dashboard("LOG", {"message": f"[{NODE_ID}] EXIT seção crítica ({request_id})"})
            finally:
                self.release(resource, request_id)
            self.log_event("COMMITTED", request_id)
            self.notify_dashboard("LOG", {"message": f"[{NODE_ID}] COMMITTED {request_id} (espera={wait_ms:.2f}ms)"})
            self.notify_dashboard("WAIT_TIME", {"wait_ms": wait_ms})
            return wait_ms


# ---------------------------------------------------------------------------
# Instância global do nó (compartilhada pelos handlers HTTP)
# ---------------------------------------------------------------------------
node = NaimiTrehelNode()


# ---------------------------------------------------------------------------
# Servidor HTTP: recebe mensagens de clientes e de outros nós do protocolo
# ---------------------------------------------------------------------------
class NodeHandler(BaseHTTPRequestHandler):
    """Handler HTTP que roteias requisições para os métodos do protocolo.

    Endpoints expostos:
      POST /client/request   — requisição de acesso a R vinda de um cliente.
      POST /protocol/request — mensagem REQUEST do algoritmo Naimi–Tréhel.
      POST /protocol/token   — mensagem TOKEN do algoritmo Naimi–Tréhel.
      GET  /status           — estado interno do nó (telemetria, para collect_metrics.py).
    """

    def log_message(self, format, *args):
        # Suprime os logs padrão do BaseHTTPRequestHandler para manter
        # o stdout exclusivamente com os eventos do protocolo.
        return

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        data   = json.loads(self.rfile.read(length).decode("utf-8"))

        if self.path == "/client/request":
            # Cliente solicita acesso exclusivo ao recurso R
            wait_ms = node.handle_client_write(data.get("resource", "R"), data["request_id"])
            resp = {
                "status":     "COMMITTED",
                "node_id":    NODE_ID,
                "request_id": data["request_id"],
                "wait_ms":    wait_ms
            }
        elif self.path == "/protocol/request":
            # Mensagem de REQUEST do algoritmo: propaga o pedido pela árvore
            node.on_receive_request(data["requester"], data["request_id"])
            resp = {"status": "OK"}
        elif self.path == "/protocol/token":
            # Mensagem de TOKEN: entrega a autorização ao nó solicitante
            node.on_receive_token(data["from"], data["request_id"])
            resp = {"status": "OK"}
        elif self.path == "/sync/set":
            # Endpoint para habilitar/desabilitar sincronização dinamicamente (Prova de Instrumentação)
            global SYNC_ENABLED
            SYNC_ENABLED = bool(data.get("enabled", True))
            print(f"[SYNC_SET] node={NODE_ID} | sync_enabled={SYNC_ENABLED}", flush=True)
            resp = {"status": "OK", "node_id": NODE_ID, "sync_enabled": SYNC_ENABLED}
        elif self.path == "/reset":
            # Endpoint para redefinir o estado distribuído ao valor inicial
            with node.state_lock:
                node.father = None if NODE_ID == "node1" else "node1"
                node.has_token = (NODE_ID == "node1")
                if node.has_token:
                    node.token_event.set()
                else:
                    node.token_event.clear()
                node.next_queue = []
                node.requesting = False
                node.protocol_msgs_sent = 0
                node.acquisitions = 0
                node.wait_times_ms.clear()
            for q in node.send_queues.values():
                while not q.empty():
                    try:
                        q.get_nowait()
                        q.task_done()
                    except Exception:
                        break
            print(f"[RESET] node={NODE_ID} | father={node.father} | has_token={node.has_token}", flush=True)
            resp = {"status": "OK", "node_id": NODE_ID, "has_token": node.has_token, "father": node.father}
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
            # Endpoint de telemetria consultado pelo collect_metrics.py
            # após cada rodada de testes para coletar métricas agregadas.
            with node.state_lock:
                sorted_waits = sorted(node.wait_times_ms)
                median_wait  = sorted_waits[len(sorted_waits) // 2] if sorted_waits else 0.0
                payload = {
                    "node_id":             NODE_ID,
                    "father":              node.father,
                    "next_queue":          node.next_queue,
                    "requesting":          node.requesting,
                    "has_token":           node.has_token,
                    "protocol_msgs_sent":  node.protocol_msgs_sent,
                    "acquisitions":        node.acquisitions,
                    "median_wait_ms":      round(median_wait, 2),
                    "sync_enabled":        SYNC_ENABLED
                }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload, indent=2).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()


if __name__ == "__main__":
    print(
        f"[INIT] {NODE_ID} iniciado | father={node.father} | token={node.has_token} | sync={SYNC_ENABLED}",
        flush=True
    )
    server = ThreadingHTTPServer(("0.0.0.0", PORT), NodeHandler)
    server.serve_forever()

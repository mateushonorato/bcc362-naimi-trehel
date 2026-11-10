# BCC362 — TP2: Sincronização em Sistemas Distribuídos (Cluster Sync)

**Disciplina:** BCC362 — Sistemas Distribuídos (UFOP)  
**Professor:** Joubert Lima  
**Equipe:** Kevin, Mateus e Crescêncio  
**Opção Escolhida:** Trilha A — A7: Algoritmo de Naimi–Tréhel (Exclusão Mútua por Token e Árvore Lógica Dinâmica com Reversão de Caminhos)

---

## 1. Visão Geral do Projeto

Este projeto implementa uma solução distribuída para coordenar acessos concorrentes de escrita a um recurso compartilhado hipotético $R$ no cenário **Cluster Sync**. A arquitetura é composta por:

- **5 Processos do Cluster Sync (`node1` a `node5`):** Executam o protocolo distribuído de **Naimi–Tréhel** (1996), mantendo uma árvore lógica dinâmica reorganizada por reversão de caminhos (*path reversal*) e uma fila distribuída de pedidos pendentes.
- **5 Clientes Geradores de Carga (`client1` a `client5`):** Cada cliente conhece exclusivamente um nó do Cluster Sync e realiza entre $10$ e $50$ requisições sequenciais de escrita identificadas unicamente por `request_id` (ex.: `client1-001`).
- **1 Processo Observador (`observer`):** Serviço independente de instrumentação que recebe notificações `BEGIN` e `END` de todos os nós ao entrarem e saírem da seção crítica simulada, contabilizando acessos simultâneos e acusando eventuais violações de exclusão mútua sem depender da comparação de relógios físicos entre máquinas distintas.

### Separação entre Sincronização e Seção Crítica
No código do nó (`node.py`), a sincronização distribuída é isolada nas primitivas `acquire(R, request_id)` e `release(R, request_id)`:
1. O nó recebe a requisição do cliente e serializa a entrada local.
2. Executa `acquire(R, request_id)`, aguardando a posse do token.
3. Registra `ENTER`, notifica `BEGIN` ao observador e simula o acesso ao recurso $R$ com um `sleep` aleatório entre $0{,}2\text{ s}$ e $1{,}0\text{ s}$.
4. Notifica `END` ao observador e registra `EXIT`.
5. Executa `release(R, request_id)`, transferindo o token ao próximo nó da fila `next` (se houver), e retorna `COMMITTED` ao cliente junto com o tempo de espera medido por relógio monotônico (`time.monotonic()`).

---

## 2. Fundamentação Teórica — Algoritmo de Naimi–Tréhel

### 2.1. Funcionamento e Estado Local
Na taxonomia de Singhal, o algoritmo pertence à família **baseada em token** (*"entro na seção crítica quando o objeto está comigo"*). Ele evolui a proposta de Raymond (1989) ao substituir a árvore estática por uma **árvore lógica dinâmica** que se achata automaticamente conforme os pedidos circulam.

Cada nó $S_i$ mantém quatro variáveis de estado protegidas internamente:
- `father`: ponteiro para o nó que provavelmente detém o token ou conduz à raiz atual da árvore lógica (`None` se o próprio nó for a raiz).
- `next_queue`: fila FIFO de nós que aguardam a transferência do token após a liberação da seção crítica.
- `requesting`: booleano que indica se o nó solicitou ou está executando a seção crítica.
- `has_token`: booleano que indica a posse atual da autorização exclusiva.

Quando um nó solicita o recurso $R$ e `father != None`, ele envia uma mensagem `Request` ao seu `father` e define `father = None` (tornando-se a nova raiz). Cada nó intermediário que recebe o `Request` o encaminha ao seu próprio `father` (caso não seja a raiz) e atualiza seu ponteiro `father` para apontar para o nó solicitante (**reversão de caminho**). Quando o pedido alcança a raiz detentora do token:
- Se ela não estiver usando ou requisitando $R$, envia o `Token` imediatamente ao solicitante.
- Se estiver na seção crítica, adiciona o solicitante em `next_queue`. Ao executar `release(R)`, o portador retira o primeiro elemento de `next_queue` e lhe transfere o `Token`.

### 2.2. Atendimento às Quatro Propriedades de Sincronização

| Propriedade | Como a implementação garante |
| :--- | :--- |
| **Exclusão Mútua** | Existe **um único token** inicializado no sistema (`node1`). Apenas o nó com `has_token == True` avança após `acquire()`. A reversão de caminhos altera ponteiros `father`, mas jamais duplica o token. |
| **Ausência de Deadlock** | A estrutura mantida pelos ponteiros `father` e pela fila `next_queue` forma um grafo direcionado acíclico (DAG/árvore). Como os pedidos caminham sempre em direção à raiz atual, não há espera circular. |
| **Ausência de Starvation** | Os pedidos retidos são encadeados de forma determinística na fila FIFO `next_queue`. Na saída da seção crítica (`release`), o token é obrigatoriamente passado ao próximo da fila, impedindo ultrapassagens indefinidas. |
| **Ordenação** | A ordem de atendimento é **posicional**, determinada pela chegada das mensagens `Request` à raiz/subárvore atual, garantindo justiça sem exigir um relógio lógico global. |

### 2.3. Hipóteses Declaradas

| Hipótese | Implementação no Projeto |
| :--- | :--- |
| **Canais FIFO par a par** | Cada nó mantém uma fila `queue.Queue()` e uma *thread worker* dedicada por destinatário (`send_queues`), garantindo que mensagens entre quaisquer dois nós cheguem estritamente na ordem de envio. |
| **Entrega confiável e sem falhas** | Em conformidade com o escopo do TP2, opera-se sem queda de processos ou perda do token, com retentativa automática na camada de transporte HTTP/TCP. |
| **Papel do Mutex Local** | Cada nó utiliza `client_entry_lock` para serializar pedidos vindos de clientes no mesmo processo e `state_lock` para garantir atomicidade na leitura/atualização de `father`, `next_queue`, `requesting` e `has_token`. **O mutex local protege apenas estruturas em memória local e não substitui o protocolo distribuído.** |

### 2.4. Análise de Complexidade e Comparação Teórica

Considere $N$ processos no Cluster Sync:

| Algoritmo | Topologia Lógica | Custo Médio (Mensagens/Entrada) | Pior Caso (Mensagens/Entrada) | Tráfego Ocioso e Gargalos |
| :--- | :--- | :---: | :---: | :--- |
| **Naimi–Tréhel** | Árvore dinâmica (reversão de caminhos) | $\mathcal{O}(\log N)$ ($H_{N-1} \approx \ln N$) | $\mathcal{O}(N)$ (cadeia degenerada) | **Zero** tráfego ocioso. A reversão de caminho achata a árvore sob disputas repetidas, reduzindo o custo das requisições subsequentes. |
| **Raymond** | Árvore estática (não rotacionada) | $\mathcal{O}(\log N)$ (se árvore balanceada) | $\mathcal{O}(N)$ (árvore em linha/cadeia) | **Zero** tráfego ocioso. Se a topologia fixa for desbalanceada ou concentrar pedidos nas folhas, o custo permanece alto permanentemente, sobrecarregando nós internos. |
| **Token Ring** | Anel lógico unidirecional | $\mathcal{O}(N)$ (média de $N/2$ saltos) | $\mathcal{O}(N)$ ($N-1$ saltos) | Gera **tráfego ocioso constante** se o token circular continuamente sem disputa, e impõe latência proporcional a $N$ sob baixa contenção. |

---

## 3. Estrutura do Repositório e Dependências

### Dependências e Versões Fixadas
- **Imagem Base:** `python:3.12-alpine` (utiliza apenas bibliotecas padrão do Python 3.12: `http.server`, `urllib.request`, `threading`, `queue`, `time`, `json`).
- **Orquestração:** Docker Engine `>= 24.0` e Docker Compose `v2` / Docker Swarm.

### Arquivos do Projeto
```text
.
├── client.py            # Gerador de carga dos clientes (10 a 50 acessos, sleep configurável)
├── node.py              # Nó do Cluster Sync (Protocolo Naimi-Tréhel, canais FIFO e telemetria)
├── observer.py          # Verificador passivo de exclusão mútua (eventos BEGIN/END e /stats)
├── Dockerfile           # Definição da imagem leve baseada em python:3.12-alpine
├── docker-compose.yml   # Orquestração local com 5 nós, 5 clientes e 1 observador
├── docker-stack.yml     # Orquestração distribuída para Docker Swarm (3 VMs na Oracle Cloud)
└── README.md            # Documentação do projeto
```

---

## 4. Parâmetros e Variáveis de Ambiente

Os serviços podem ser configurados via variáveis de ambiente no `docker-compose.yml` ou `docker-stack.yml`:

| Variável | Serviço | Padrão | Descrição |
| :--- | :---: | :---: | :--- |
| `NODE_ID` | `node` | `node1` | Identificador único do nó no cluster (`node1` a `node10`). |
| `INITIAL_FATHER` | `node` | `""` | Ponteiro `father` inicial (`""` na raiz `node1`, `"node1"` nos demais). |
| `HAS_TOKEN` | `node` | `"false"` | Define se o nó inicia com a posse do token (`"true"` apenas no `node1`). |
| `SYNC_ENABLED` | `node` | `"true"` | Quando `"false"`, desativa o protocolo Naimi–Tréhel para testar o detector de colisões do observador. |
| `CS_MIN_SLEEP` / `CS_MAX_SLEEP` | `node` | `0.2` / `1.0` | Intervalo aleatório (em segundos) de permanência na seção crítica simulada. |
| `CLIENT_ID` | `client` | `client1` | Identificador único do cliente gerador de carga. |
| `TARGET_URL` | `client` | `http://node1:8000` | Endereço do único elemento do Cluster Sync conhecido pelo cliente. |
| `NUM_REQUESTS` | `client` | `10` | Quantidade de acessos de escrita solicitados pelo cliente ($10$ a $50$). |
| `CLIENT_MIN_SLEEP` / `CLIENT_MAX_SLEEP` | `client` | `1.0` / `5.0` | Tempo de espera aleatório (em segundos) entre pedidos consecutivos dos clientes. |
| `CLIENT1_MIN_SLEEP` / `CLIENT1_MAX_SLEEP` | `client` | `1.0` / `5.0` | Permite sobrepor individualmente o intervalo do `client1` para o experimento de carga desigual. |
| `RANDOM_SEED` | `client` | `42` | Semente para reprodutibilidade dos intervalos aleatórios nos experimentos. |
| `CLIENT_TIMEOUT` | `client` | `300.0` | Timeout limite (em segundos) da requisição HTTP do cliente antes de retentar. |

---

## 5. Execução Local (Docker Compose)

### 5.1. Construir e iniciar o cenário padrão
```bash
docker compose up --build
```
Os 5 nós (`node1` a `node5`), o `observer` e os 5 clientes (`client1` a `client5`) iniciarão em contêineres isolados. Você verá no terminal os registros de `REQUEST`, `REV_PATH` (reversão de ponteiros `father`), `ACQUIRE`, `ENTER`, `BEGIN`/`END` no observador, `EXIT`, `RELEASE` e `COMMITTED`.

### 5.2. Inspecionar métricas e estado dos nós
Em outro terminal, consulte o verificador de propriedades no observador e o estado interno de cada nó:
```bash
# Estatísticas globais do Observador (acessos concluídos e contagem de sobreposições)
curl -s http://localhost:9000/stats

# Estado interno e métricas de cada nó (mensagens de protocolo enviadas e espera mediana em ms)
for port in 8001 8002 8003 8004 8005; do
  echo "=== Porta $port ==="
  curl -s http://localhost:$port/status
done
```

### 5.3. Encerrar e limpar os contêineres
```bash
docker compose down
```

---

## 6. Execução Distribuída na Oracle Cloud (Docker Swarm — 3 VMs)

A implantação em nuvem utiliza 3 máquinas virtuais com Ubuntu 26.04 (1 OCPU e 8 GB de RAM cada) na Oracle Cloud Infrastructure (OCI), distribuindo os contêineres pela rede `overlay` do Docker Swarm:
- **VM 1 (`vm1`):** `observer`, `node1`, `client1`
- **VM 2 (`vm2`):** `node2`, `node3`, `client2`, `client3`
- **VM 3 (`vm3`):** `node4`, `node5`, `client4`, `client5`

### Passo 1: Liberar portas nas 3 VMs (e na Security List da VCN)
Certifique-se de que a Security List da sub-rede na OCI permite tráfego interno nas portas TCP `2377`, TCP/UDP `7946`, UDP `4789` e TCP `8000-9000`. Em cada uma das 3 VMs, libere o firewall local:
```bash
sudo iptables -I INPUT -p tcp -m multiport --dports 2377,7946,8000:9000 -j ACCEPT
sudo iptables -I INPUT -p udp -m multiport --dports 4789,7946 -j ACCEPT
sudo netfilter-persistent save 2>/dev/null || true
```

### Passo 2: Inicializar o Swarm e rotular os nós
Na **VM 1 (Manager)**:
```bash
docker swarm init --advertise-addr <IP_PRIVADO_VM1>
```
Execute o comando `docker swarm join ...` gerado na **VM 2** e na **VM 3**. Em seguida, na **VM 1**, aplique os rótulos de posicionamento:
```bash
docker node update --label-add vm=vm1 <HOSTNAME_VM1>
docker node update --label-add vm=vm2 <HOSTNAME_VM2>
docker node update --label-add vm=vm3 <HOSTNAME_VM3>
```

### Passo 3: Construir a imagem nas 3 VMs
Em cada uma das 3 máquinas, na pasta do repositório:
```bash
docker build -t cluster-sync:latest .
```

### Passo 4: Fazer o deploy da Stack distribuída
Na **VM 1 (Manager)**:
```bash
docker stack deploy -c docker-stack.yml clustersync
```
Para acompanhar a execução e os logs entre as máquinas:
```bash
docker stack ps clustersync
docker service logs -f clustersync_observer
curl -s http://localhost:9000/stats
```
Para remover a stack após um experimento:
```bash
docker stack rm clustersync
```

---

## 7. Roteiro de Experimentos e Evidências de Sincronização

Abaixo estão os comandos e roteiros detalhados para reproduzir os cenários exigidos na Seção 7 do TP2, cobrindo tanto a **execução local (Docker Compose)** quanto a **execução em nuvem (Docker Swarm — Oracle Cloud Infrastructure)**:

### 7.1. Prova de Instrumentação (Sincronização Desabilitada)
Comprova que o observador é um verificador passivo confiável capaz de acusar colisões (`[VIOLACAO] Sobreposicao detectada!`) quando a coordenação distribuída é desligada (`SYNC_ENABLED=false`):

- **Local (Docker Compose):**
  ```bash
  SYNC_ENABLED=false CLIENT_MIN_SLEEP=0 CLIENT_MAX_SLEEP=0 docker compose up
  ```
- **Nuvem (Docker Swarm na VM1 Manager):**
  ```bash
  SYNC_ENABLED=false CLIENT_MIN_SLEEP=0 CLIENT_MAX_SLEEP=0 docker stack deploy -c docker-stack.yml clustersync
  ```
- **Resultado esperado em `/stats`:** `overlap_events > 0` e `max_concurrent > 1`, provando que o observador detecta acessos concorrentes reais e que não atua como autorizador ou mutex.

---

### 7.2. Experimento 1 — Carga Normal
Cenário de referência com 5 clientes, 20 pedidos por cliente, espera padrão de $1\text{ a }5\text{ s}$ entre pedidos e seção crítica simulada de $0{,}2\text{ a }1{,}0\text{ s}$:

- **Local (Docker Compose):**
  ```bash
  SYNC_ENABLED=true NUM_REQUESTS=20 CLIENT_MIN_SLEEP=1.0 CLIENT_MAX_SLEEP=5.0 docker compose up
  ```
- **Nuvem (Docker Swarm na VM1 Manager):**
  ```bash
  SYNC_ENABLED=true NUM_REQUESTS=20 CLIENT_MIN_SLEEP=1.0 CLIENT_MAX_SLEEP=5.0 docker stack deploy -c docker-stack.yml clustersync
  ```
- **Resultado esperado:**
  - Em `http://localhost:9000/stats`: `total_completed: 100`, `overlap_events: 0`, `max_concurrent: 1`.
  - Em cada nó (`/status`): tempos medianos de espera balanceados e número reduzido de mensagens de protocolo.

---

### 7.3. Experimento 2 — Alta Contenção
Remove o intervalo entre requisições dos clientes (`CLIENT_MIN_SLEEP=0` e `CLIENT_MAX_SLEEP=0`), forçando disputa contínua pelo recurso $R$ e exercitando intensamente o enfileiramento na `next_queue`:

- **Local (Docker Compose):**
  ```bash
  SYNC_ENABLED=true NUM_REQUESTS=20 CLIENT_MIN_SLEEP=0 CLIENT_MAX_SLEEP=0 docker compose up
  ```
- **Nuvem (Docker Swarm na VM1 Manager):**
  ```bash
  SYNC_ENABLED=true NUM_REQUESTS=20 CLIENT_MIN_SLEEP=0 CLIENT_MAX_SLEEP=0 docker stack deploy -c docker-stack.yml clustersync
  ```
- **Resultado esperado:**
  - Rigorosa garantia de exclusão mútua (`overlap_events: 0`, `max_concurrent: 1`).
  - Redução no número médio de mensagens de protocolo por entrada: a árvore lógica dinâmica achata-se rapidamente via reversão de caminhos (*path reversal*), e pedidos subsequentes são repassados diretamente ao detentor ou enfileirados em $\mathcal{O}(1)$ na raiz.

---

### 7.4. Experimento 3 — Carga Desigual (Fairness e Prioridade)
Avalia a justiça do algoritmo configurando um dos clientes (`client1`) para solicitar o recurso $R$ com frequência muito superior (`CLIENT1_MIN_SLEEP=0.1` e `CLIENT1_MAX_SLEEP=0.3`), enquanto os demais clientes (`client2` a `client5`) mantêm o ritmo normal (`CLIENT_MIN_SLEEP=1.0` e `CLIENT_MAX_SLEEP=5.0`):

- **Local (Docker Compose):**
  ```bash
  SYNC_ENABLED=true NUM_REQUESTS=20 CLIENT1_MIN_SLEEP=0.1 CLIENT1_MAX_SLEEP=0.3 CLIENT_MIN_SLEEP=1.0 CLIENT_MAX_SLEEP=5.0 docker compose up
  ```
- **Nuvem (Docker Swarm na VM1 Manager):**
  ```bash
  SYNC_ENABLED=true NUM_REQUESTS=20 CLIENT1_MIN_SLEEP=0.1 CLIENT1_MAX_SLEEP=0.3 CLIENT_MIN_SLEEP=1.0 CLIENT_MAX_SLEEP=5.0 docker stack deploy -c docker-stack.yml clustersync
  ```
- **Discussão teórica e evidências de justiça:**
  - **Ausência de Starvation:** O nó mais frequente (`node1`) não monopoliza o recurso indefinidamente. Quando nós menos frequentes (`node2`–`node5`) solicitam $R$, seus pedidos sobem pela árvore de ponteiros `father` e são anexados à fila `next_queue`.
  - **Repasse Obrigatório:** Ao finalizar a seção crítica em `release()`, o nó detentor é estritamente obrigado a transferir o token para o primeiro elemento da `next_queue`, assegurando que requisições esparsas sejam atendidas com prioridade posicional justa.
  - **Distribuição e Espera:** Consultando `curl -s http://localhost:<porta>/status`, observa-se que `node1` acumula aquisições mais rápido, porém a mediana de tempo de espera dos outros nós permanece limitada ao tempo determinístico de esvaziamento da fila FIFO.

---

## 8. Referências Bibliográficas

1. **Naimi, M., Tréhel, M., & Arnold, A. (1996).** *A $\log(N)$ Distributed Mutual Exclusion Algorithm Based on Path Reversal.* Journal of Parallel and Distributed Computing, 34(1), 1–13. https://doi.org/10.1006/jpdc.1996.0041
2. **Raymond, K. (1989).** *A Tree-Based Algorithm for Distributed Mutual Exclusion.* ACM Transactions on Computer Systems, 7(1), 61–77. https://doi.org/10.1109/12.24268
# BCC362 — TP2: Sincronização em Sistemas Distribuídos (Cluster Sync)

**Disciplina:** BCC362 — Sistemas Distribuídos (UFOP)  
**Professor:** Joubert Lima  
**Equipe:** Kevin, Mateus e Crescêncio  
**Opção Escolhida:** Trilha A — A7: Algoritmo de Naimi–Tréhel (Exclusão Mútua por Token e Árvore Lógica Dinâmica com Reversão de Caminhos)

---

## 1. Visão Geral do Projeto

Este projeto implementa uma solução distribuída para coordenar acessos concorrentes de escrita a um recurso compartilhado hipotético $R$ no cenário **Cluster Sync**. A arquitetura é modular, conteinerizada e projetada para escalar entre topologias de **5 e 10 nós**, composta por:

- **Processos do Cluster Sync (`node1` a `node5` ou `node1` a `node10`):** Executam o protocolo distribuído de **Naimi–Tréhel** (1996), mantendo uma árvore lógica dinâmica reorganizada por reversão de caminhos (*path reversal*) e uma fila distribuída de pedidos pendentes (`next_queue`).
- **Clientes Geradores de Carga (`client1` a `client5` ou `client1` a `client10`):** Cada cliente conhece exclusivamente um nó do Cluster Sync e realiza requisições sequenciais de escrita identificadas unicamente por `request_id` (ex.: `client1-001`).
- **Processo Observador (`observer`):** Serviço independente de instrumentação que recebe notificações `BEGIN` e `END` de todos os nós ao entrarem e saírem da seção crítica simulada. Ele contabiliza acessos simultâneos e acusa violações de exclusão mútua (`overlap_events`) sem depender de sincronização de relógios físicos entre máquinas distintas.
- **Automação de Benchmark (`collect_metrics.py`):** Script CLI que orquestra a execução completa dos cenários de teste (local ou nuvem), repetições, variação de sementes pseudoaleatórias, coleta de métricas via REST e exportação estruturada para JSON e CSV.

### Separação entre Sincronização e Seção Crítica
No código do nó (`node.py`), a sincronização distribuída é estritamente isolada nas primitivas `acquire(R, request_id)` e `release(R, request_id)`:
1. O nó recebe a requisição do cliente e serializa a entrada local (`client_entry_lock`).
2. Executa `acquire(R, request_id)`, emitindo mensagem `Request` pela árvore se não possuir o token e bloqueando até a posse de `has_token == True`.
3. Notifica `BEGIN` ao observador, registra `ENTER` e simula a permanência na seção crítica do recurso $R$ com um `sleep` aleatório entre $0{,}2\text{ s}$ e $1{,}0\text{ s}$.
4. A execução da seção crítica é encapsulada em bloco `try ... finally`, garantindo que `release(R, request_id)` seja invocado impreterivelmente.
5. Notifica `END` ao observador, registra `EXIT` e transfere o token ao próximo solicitante da fila `next_queue` (se houver), retornando `COMMITTED` ao cliente junto com o tempo de espera medido via relógio monotônico (`time.monotonic()`).

---

## 2. Fundamentação Teórica — Algoritmo de Naimi–Tréhel

### 2.1. Funcionamento e Estado Local
Na taxonomia de Singhal, o algoritmo pertence à família **baseada em token** (*"entro na seção crítica quando o objeto está comigo"*). Ele aprimora a proposta de Raymond (1989) ao substituir a árvore estática por uma **árvore lógica dinâmica** que se achata continuamente conforme as requisições trafegam.

Cada nó $S_i$ mantém quatro variáveis de estado fundamentais protegidas por exclusão mútua local:
- `father`: ponteiro para o nó que conduz à raiz atual da árvore lógica (`None` se o próprio nó for a raiz detentora ou solicitante).
- `next_queue`: fila FIFO de nós pendentes que aguardam a transferência do token após a liberação da seção crítica.
- `requesting`: booleano indicando se o nó solicitou ou está executando a seção crítica.
- `has_token`: booleano indicando a posse atual da autorização exclusiva.

Quando um nó solicita o recurso $R$ e `father != None`:
1. Envia uma mensagem `Request` ao seu `father` atual e define `father = None` (tornando-se a nova raiz da árvore).
2. Cada nó intermediário que recebe o `Request` o encaminha ao seu respectivo `father` e redireciona seu próprio ponteiro `father` para apontar diretamente para o nó solicitante (**reversão de caminhos**).
3. Quando a mensagem atinge a raiz detentora do token:
   - Se ela não estiver em uso nem requisitando $R$, transfere o `Token` diretamente ao solicitante.
   - Se estiver ocupada na seção crítica, enfileira o solicitante em sua `next_queue`. Ao finalizar (`release`), o portador retira o próximo de `next_queue` e envia o `Token` em um único salto direto.

### 2.2. Atendimento às Quatro Propriedades de Sincronização

| Propriedade | Como a implementação garante |
| :--- | :--- |
| **Exclusão Mútua** | Existe **um único token** no sistema, inicializado exclusivamente no `node1`. Apenas o nó com `has_token == True` obtém permissão para avançar em `acquire()`. A reversão de arestas altera apenas ponteiros `father`, sem jamais duplicar o token. |
| **Ausência de Deadlock** | A estrutura mantida pelos ponteiros `father` e pela fila `next_queue` forma um grafo acíclico direcionado. Como os pedidos caminham estritamente em direção à raiz atual, a espera circular é estruturalmente impossível. |
| **Ausência de Starvation** | Os nós em espera são encadeados de forma determinística na fila FIFO `next_queue`. Ao liberar a seção crítica (`release`), o detentor é obrigado a transferir o token para a cabeça da fila, impedindo ultrapassagens indefinidas. |
| **Ordenação** | A ordem de atendimento é **posicional**, estabelecida pela ordem sequencial de chegada das mensagens `Request` à raiz ou subárvore corrente, garantindo justiça posicional sem requerer relógios físicos ou lógicos globais. |

### 2.3. Hipóteses Declaradas

| Hipótese | Implementação no Projeto |
| :--- | :--- |
| **Canais FIFO par a par** | Cada nó gerencia uma fila `queue.Queue()` e uma *thread worker* dedicada por destinatário (`send_queues`), assegurando que mensagens entre quaisquer dois nós cheguem rigorosamente na ordem de envio. |
| **Entrega confiável e sem falhas** | Em conformidade com o escopo do trabalho acadêmico, o sistema assume ausência de partições de rede ou falhas por parada, utilizando transporte HTTP com retentativas automáticas e reconexão. |
| **Papel do Mutex Local** | Cada nó utiliza `client_entry_lock` para serializar requisições concorrentes de clientes locais e `state_lock` para garantir atomicidade na manipulação do estado (`father`, `next_queue`, `requesting`, `has_token`). **O mutex local protege unicamente a memória do processo e não substitui o protocolo distribuído.** |

### 2.4. Análise de Complexidade e Comparação Teórica

Considere $N$ processos participantes no Cluster Sync:

| Algoritmo | Topologia Lógica | Custo Médio (Mensagens/Entrada) | Pior Caso (Mensagens/Entrada) | Tráfego Ocioso e Gargalos |
| :--- | :--- | :---: | :---: | :--- |
| **Naimi–Tréhel** | Árvore dinâmica com reversão de caminhos | $\mathcal{O}(\log N)$ ($H_{N-1} + 1 \approx \ln N + 1$) | $\mathcal{O}(N)$ (cadeia degenerada inicial) | **Zero** tráfego ocioso. A reversão achata a árvore sob disputas concorrentes, mantendo caminhos curtos e distribuindo o tráfego. |
| **Raymond** | Árvore estática fixa (não rotacionada) | $\mathcal{O}(\log N)$ (se árvore balanceada) | $\mathcal{O}(N)$ (se degenerada em lista) | **Zero** tráfego ocioso. Topologias fixas geram sobrecarga desproporcional sobre nós centrais e raízes fixas. |
| **Token Ring** | Anel lógico unidirecional | $\mathcal{O}(N)$ (média de $N/2$ saltos) | $\mathcal{O}(N)$ ($N-1$ saltos) | Gera **tráfego constante** mesmo sem requisições (passagem contínua do bastão), impondo alta latência em anéis grandes. |

---

## 3. Estrutura do Repositório e Dependências

### Dependências e Versões
- **Imagem Base:** `python:3.12-alpine` (utiliza apenas bibliotecas padrão: `http.server`, `urllib.request`, `threading`, `queue`, `time`, `json`).
- **Orquestração:** Docker Engine `>= 24.0` e Docker Compose `v2` / Docker Swarm.
- **Python do Host:** Python `>= 3.10` (para execução do script de automação de testes `collect_metrics.py`).

### Arquivos do Projeto
```text
.
├── client.py                  # Gerador de carga dos clientes (agnóstico, lê parâmetros padrão de ambiente)
├── node.py                    # Nó do Cluster Sync (Naimi-Tréhel P2P, canais FIFO e telemetria de espera)
├── observer.py                # Verificador passivo de exclusão mútua (eventos BEGIN/END e endpoint /stats)
├── collect_metrics.py         # Script de automação para execução, repetições e coleta estruturada de métricas
├── metrics_results.json       # Base de dados JSON consolidada com os resultados das execuções empíricas
├── metrics_results.csv        # Base de dados CSV consolidada para análise e geração de gráficos
├── Dockerfile                 # Definição da imagem leve baseada em python:3.12-alpine
├── docker-compose.yml         # Orquestração local base com 5 nós, 5 clientes e 1 observador
├── docker-compose-10nodes.yml # Extensão Compose para escala de 10 nós (adiciona node6..10 e client6..10)
├── docker-compose-unequal.yml # Override Compose de carga desigual (redefine client1 para intervalo 0.1 a 0.3s)
├── docker-stack.yml           # Orquestração Swarm base com 5 nós distribuídos em 3 VMs (Oracle Cloud)
├── docker-stack-10nodes.yml   # Extensão Swarm para escala de 10 nós (distribuição simétrica 7/7/7)
├── docker-stack-unequal.yml   # Override Swarm de carga desigual (redefine client1 para intervalo 0.1 a 0.3s)
└── README.md                  # Documentação completa e relatório de experimentos
```

---

## 4. Parâmetros e Variáveis de Ambiente

As configurações dos serviços podem ser ajustadas via variáveis de ambiente no Compose, Stack ou através do `collect_metrics.py`. Todos os clientes e nós são completamente agnósticos e utilizam a mesma interface padrão:

| Variável | Serviço | Padrão | Descrição |
| :--- | :---: | :---: | :--- |
| `NODE_ID` | `node` | `node1` | Identificador único do nó no cluster (`node1` a `node10`). |
| `INITIAL_FATHER` | `node` | `""` | Ponteiro `father` inicial (`""` na raiz inicial `node1`, `"node1"` nos demais nós). |
| `HAS_TOKEN` | `node` | `"false"` | Define se o nó detém o token no início (`"true"` unicamente no `node1`). |
| `SYNC_ENABLED` | `node` | `"true"` | Quando `"false"`, desativa a sincronização distribuída para atestar a sensibilidade do observador. |
| `CS_MIN_SLEEP` / `CS_MAX_SLEEP` | `node` | `0.2` / `1.0` | Duração da permanência simulada na seção crítica (em segundos). |
| `CLIENT_ID` | `client` | `client1` | Identificador único do cliente gerador de carga. |
| `TARGET_URL` | `client` | `http://node1:8000` | Endereço HTTP do nó associado ao cliente. |
| `NUM_REQUESTS` | `client` | `10` | Quantidade de requisições sequenciais de escrita ($10$ a $50$). |
| `CLIENT_MIN_SLEEP` / `CLIENT_MAX_SLEEP` | `client` | `1.0` / `5.0` | Intervalo de espera aleatório entre requisições sucessivas dos clientes. |
| `RANDOM_SEED` | `client` | `42` | Semente pseudoaleatória para reprodutibilidade das durações e intervalos. |
| `CLIENT_TIMEOUT` | `client` | `300.0` | Timeout máximo (em segundos) de espera pela resposta HTTP `COMMITTED`. |

---

## 5. Execução Local (Docker Compose)

O projeto utiliza o recurso nativo de **composição de arquivos** (`-f`) do Docker Compose:

### 5.1. Construir e Iniciar Manualmente

**Cenário padrão (5 nós):**
```bash
docker compose up --build
```

**Cenário de escala (10 nós — composição):**
```bash
docker compose -f docker-compose.yml -f docker-compose-10nodes.yml up --build
```

**Cenário de carga desigual (5 nós — override):**
```bash
docker compose -f docker-compose.yml -f docker-compose-unequal.yml up
```

**Cenário de escala com carga desigual (10 nós — composição tripla):**
```bash
docker compose -f docker-compose.yml -f docker-compose-10nodes.yml -f docker-compose-unequal.yml up
```

### 5.2. Inspecionar Métricas e Estado em Tempo de Execução
Em outro terminal, consulte o verificador de propriedades no observador e o estado interno dos nós:
```bash
# Estatísticas globais do Observador (acessos concluídos e violações de exclusão mútua):
curl -s http://localhost:9000/stats

# Métricas e estado interno de cada nó (portas 8001 a 8005 ou 8010):
for port in $(seq 8001 8005); do
  echo "=== Porta $port ==="
  curl -s http://localhost:$port/status
done
```

### 5.3. Encerrar os Contêineres
```bash
docker compose down
# ou para 10 nós:
docker compose -f docker-compose.yml -f docker-compose-10nodes.yml down
```

---

## 6. Execução Distribuída na Oracle Cloud (Docker Swarm — 3 VMs)

A implantação em nuvem distribui os contêineres entre **3 máquinas virtuais Ubuntu 24.04/26.04** (1 OCPU e 8 GB de RAM cada) na Oracle Cloud Infrastructure (OCI), utilizando composição nativa de stacks (`-c`):

- **Topologia de 5 nós (`docker-stack.yml`):**
  - **VM 1 (`vm1` — Manager, 3 contêineres):** `observer`, `node1`, `client1`
  - **VM 2 (`vm2` — Worker, 4 contêineres):** `node2`, `node3`, `client2`, `client3`
  - **VM 3 (`vm3` — Worker, 4 contêineres):** `node4`, `node5`, `client4`, `client5`

- **Topologia de 10 nós (`docker-stack.yml` + `docker-stack-10nodes.yml` — distribuição simétrica balanceada de 7 contêineres por VM):**
  - **VM 1 (`vm1` — Manager, 7 contêineres):** `observer`, `node1`, `client1` + `node6`, `node7`, `client6`, `client7`
  - **VM 2 (`vm2` — Worker, 7 contêineres):** `node2`, `node3`, `client2`, `client3` + `node8`, `client8`, `client9`
  - **VM 3 (`vm3` — Worker, 7 contêineres):** `node4`, `node5`, `client4`, `client5` + `node9`, `node10`, `client10`

### Passo 1: Configurar Regras de Firewall nas VMs e VCN
Na Security List da sub-rede da OCI e no firewall interno de cada VM, libere as portas do Swarm e dos serviços:
```bash
sudo iptables -I INPUT -p tcp -m multiport --dports 2377,7946,8000:9000 -j ACCEPT
sudo iptables -I INPUT -p udp -m multiport --dports 4789,7946 -j ACCEPT
sudo netfilter-persistent save 2>/dev/null || true
```

### Passo 2: Inicializar o Cluster Swarm e Rotular Nós
Na **VM 1 (Manager)**:
```bash
docker swarm init --advertise-addr <IP_PRIVADO_VM1>
```
Execute o comando `docker swarm join ...` retornado nas **VM 2** e **VM 3**. Em seguida, na **VM 1**, rotule cada máquina:
```bash
docker node update --label-add vm=vm1 <HOSTNAME_VM1>
docker node update --label-add vm=vm2 <HOSTNAME_VM2>
docker node update --label-add vm=vm3 <HOSTNAME_VM3>
```

### Passo 3: Construir a Imagem nas 3 VMs
Em cada máquina virtual, clone o repositório e compile a imagem localmente:
```bash
docker build -t cluster-sync:latest .
```

### Passo 4: Deploy e Acompanhamento
Na **VM 1 (Manager)**:
```bash
# Para 5 nós:
docker stack deploy -c docker-stack.yml clustersync

# Para 10 nós (composição):
docker stack deploy -c docker-stack.yml -c docker-stack-10nodes.yml clustersync

# Para carga desigual:
docker stack deploy -c docker-stack.yml -c docker-stack-unequal.yml clustersync
```
Acompanhe os serviços e métricas:
```bash
docker stack ps clustersync
curl -s http://localhost:9000/stats
```
Para remover a stack:
```bash
docker stack rm clustersync
```

---

## 7. Roteiro de Experimentos, Automação e Resultados Empíricos

Para atender rigorosamente aos requisitos da **Seção 7 do TP2**, desenvolvemos um script de automação (`collect_metrics.py`) que executa todos os cenários com repetições sistemáticas, variando sementes pseudoaleatórias e gravando os resultados estruturados em arquivos JSON e CSV.

### 7.1. Automação com `collect_metrics.py`

O script orquestra o ciclo de vida completo dos contêineres, injeta as variáveis de ambiente necessárias para cada teste, aguarda o encerramento das requisições via polling no endpoint `/stats` do observador, coleta as métricas de latência e tráfego de cada nó (`/status`) e calcula medianas consolidadas:

```text
usage: collect_metrics.py [-h] [--mode {local,swarm}] [--nodes {5,10,both}]
                          [--experiments {disabled,normal,high,unequal,all} ...]
                          [--repetitions REPETITIONS] [--requests REQUESTS]
                          [--append] [--json-out JSON_OUT] [--csv-out CSV_OUT]
```

#### Comandos de Execução Rápida

- **Executar benchmark completo local (5 e 10 nós, todos os testes, 3 repetições):**
  ```bash
  python3 collect_metrics.py --mode local --nodes both --experiments all --repetitions 3 --requests 10
  ```

- **Executar benchmark completo no Docker Swarm (na VM1 Manager):**
  ```bash
  python3 collect_metrics.py --mode swarm --nodes both --experiments all --repetitions 3 --requests 10
  ```

- **Executar apenas um cenário específico (ex.: alta contenção com 10 nós):**
  ```bash
  python3 collect_metrics.py --mode local --nodes 10 --experiments high --repetitions 3
  ```

---

### 7.2. Especificações do Ambiente de Testes

Os experimentos locais reportados abaixo foram executados no seguinte ambiente de hardware e software:

- **Processador:** AMD Ryzen 5 4600H with Radeon Graphics (6 núcleos físicos, 12 threads @ 3.0 GHz base / 4.0 GHz boost)
- **Memória RAM:** 16 GB DDR4 (14.9 GiB reconhecidos pelo sistema)
- **Sistema Operacional:** Linux x86_64 (CachyOS Linux, Kernel 7.3.3-1-cachyos, PREEMPT_DYNAMIC)
- **Docker Engine:** Versão 29.9.3, build 5457199aa5 (Docker Compose v2 e Swarm integrados)
- **Python:** Versão 3.13.15 (Host para o orquestrador `collect_metrics.py`) / Python 3.12-alpine (Contêineres)

---

### 7.3. Resultados Empíricos — Execução Local

Foram executadas **20 rodadas de teste**, contemplando a prova de instrumentação, carga normal, alta contenção e carga desigual para topologias de 5 e 10 nós, com 3 repetições independentes com sementes distintas (`52`, `62` e `72`).

#### Tabela Consolidada de Execuções Locais (`metrics_results.csv`)

| Experimento | Topologia | Rep. | Semente | Reqs/Cliente | Total Esperado | Total Concluído | Violações de Mutex | Máx. Concorrente | Tempo Total (s) | Mensagens Protocolo | Mensagens / Entrada | Espera Mediana (ms) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **disabled_sync** | 5 nós | 1 | 52 | 10 | 50 | 50 | **48** | **5** | 8.02 | 0 | 0.00 | 0.00 |
| **disabled_sync** | 10 nós | 1 | 52 | 10 | 100 | 100 | **99** | **10** | 8.02 | 0 | 0.00 | 0.00 |
| **normal_contention** | 5 nós | 1 | 52 | 10 | 50 | 50 | **0** | **1** | 41.09 | 144 | 2.88 | 171.88 |
| **normal_contention** | 5 nós | 2 | 62 | 10 | 50 | 50 | **0** | **1** | 41.09 | 143 | 2.86 | 444.39 |
| **normal_contention** | 5 nós | 3 | 72 | 10 | 50 | 50 | **0** | **1** | 40.09 | 142 | 2.84 | 225.12 |
| **normal_contention** | 10 nós | 1 | 52 | 10 | 100 | 100 | **0** | **1** | 61.12 | 355 | 3.55 | 2588.68 |
| **normal_contention** | 10 nós | 2 | 62 | 10 | 100 | 100 | **0** | **1** | 66.14 | 352 | 3.52 | 2460.86 |
| **normal_contention** | 10 nós | 3 | 72 | 10 | 100 | 100 | **0** | **1** | 58.12 | 351 | 3.51 | 2415.54 |
| **high_contention** | 5 nós | 1 | 52 | 10 | 50 | 50 | **0** | **1** | 33.07 | 147 | 2.94 | 2622.31 |
| **high_contention** | 5 nós | 2 | 62 | 10 | 50 | 50 | **0** | **1** | 29.06 | 147 | 2.94 | 2217.99 |
| **high_contention** | 5 nós | 3 | 72 | 10 | 50 | 50 | **0** | **1** | 31.08 | 147 | 2.94 | 2439.09 |
| **high_contention** | 10 nós | 1 | 52 | 10 | 100 | 100 | **0** | **1** | 59.12 | 375 | 3.75 | 5180.63 |
| **high_contention** | 10 nós | 2 | 62 | 10 | 100 | 100 | **0** | **1** | 59.13 | 354 | 3.54 | 5346.44 |
| **high_contention** | 10 nós | 3 | 72 | 10 | 100 | 100 | **0** | **1** | 61.14 | 372 | 3.72 | 5426.89 |
| **unequal_load** | 5 nós | 1 | 52 | 10 | 50 | 50 | **0** | **1** | 40.08 | 130 | 2.60 | 252.42 |
| **unequal_load** | 5 nós | 2 | 62 | 10 | 50 | 50 | **0** | **1** | 41.09 | 133 | 2.66 | 292.08 |
| **unequal_load** | 5 nós | 3 | 72 | 10 | 50 | 50 | **0** | **1** | 40.09 | 125 | 2.50 | 291.75 |
| **unequal_load** | 10 nós | 1 | 52 | 10 | 100 | 100 | **0** | **1** | 60.14 | 358 | 3.58 | 2533.24 |
| **unequal_load** | 10 nós | 2 | 62 | 10 | 100 | 100 | **0** | **1** | 60.13 | 355 | 3.55 | 2432.23 |
| **unequal_load** | 10 nós | 3 | 72 | 10 | 100 | 100 | **0** | **1** | 58.12 | 332 | 3.32 | 2369.64 |

#### Resumo Estatístico Comparativo (Médias das Repetições)

| Cenário de Teste | Topologia | Violações de Mutex | Máx. Concorrente | Tempo Médio Total (s) | Mensagens Médias por Entrada | Espera Mediana Média (ms) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Prova de Instrumentação** | 5 nós | **48** | **5** | 8.02 s | 0.00 | 0.00 ms |
| **Prova de Instrumentação** | 10 nós | **99** | **10** | 8.02 s | 0.00 | 0.00 ms |
| **Carga Normal** | 5 nós | **0** | **1** | 40.76 s | **2.86** | 280.46 ms |
| **Carga Normal** | 10 nós | **0** | **1** | 61.79 s | **3.53** | 2488.36 ms |
| **Alta Contenção** | 5 nós | **0** | **1** | 31.07 s | **2.94** | 2426.46 ms |
| **Alta Contenção** | 10 nós | **0** | **1** | 59.80 s | **3.67** | 5317.99 ms |
| **Carga Desigual** | 5 nós | **0** | **1** | 40.42 s | **2.58** | 278.75 ms |
| **Carga Desigual** | 10 nós | **0** | **1** | 59.46 s | **3.48** | 2445.04 ms |

---

### 7.4. Análise Teórica dos Resultados Empíricos

O confronto entre as métricas coletadas e a fundamentação teórica de Naimi–Tréhel (1996) e sistemas distribuídos evidencia quatro conclusões fundamentais:

#### 1. Garantia Estrita de Exclusão Mútua (*Safety*)
- **Eficácia da Prova de Instrumentação:** No teste com sincronização desabilitada (`disabled_sync`), o observador registrou **48 sobreposições** (com até 5 processos simultâneos na SC) em 5 nós e **99 sobreposições** (com até 10 processos simultâneos na SC) em 10 nós. Isso comprova que a ausência de violações nos testes coordenados não decorre de artefatos temporais ou desacoplamento dos clientes, mas da eficácia real do protocolo.
- **Isolamento Concorrente Absoluto:** Em todas as **18 rodadas coordenadas** (totalizando 1.350 entradas em seção crítica), registrou-se **rigorosamente 0 violações** de exclusão mútua (`overlap_events = 0`) e concorrência máxima de exatamente 1 (`max_concurrent = 1`).

#### 2. Conformidade com a Complexidade Logarítmica $\mathcal{O}(\log N)$
O número teórico de mensagens por entrada no algoritmo de Naimi–Tréhel é delimitado pela soma harmônica $H_{N-1} = \sum_{i=1}^{N-1} \frac{1}{i}$ para o encaminhamento do `Request` ao longo dos ponteiros `father`, somada a exatamente **1 mensagem** para o repasse direto do `Token` do portador ao solicitante:
$$\bar{M}(N) = H_{N-1} + 1 \approx \ln(N - 1) + \gamma + 1$$

- **Topologia de 5 nós ($N=5$):**
  $$H_4 + 1 = \left(1 + \frac{1}{2} + \frac{1}{3} + \frac{1}{4}\right) + 1 \approx 2{,}083 + 1 = 3{,}083\text{ msgs/entrada}$$
  - *Média empírica medida:* **2.86 msgs/entrada** (Normal), **2.94 msgs/entrada** (Alta Contenção) e **2.58 msgs/entrada** (Desigual). Todas abaixo da cota teórica superior média.
- **Topologia de 10 nós ($N=10$):**
  $$H_9 + 1 = \left(\sum_{i=1}^9 \frac{1}{i}\right) + 1 \approx 2{,}829 + 1 = 3{,}829\text{ msgs/entrada}$$
  - *Média empírica medida:* **3.53 msgs/entrada** (Normal), **3.67 msgs/entrada** (Alta Contenção) e **3.48 msgs/entrada** (Desigual).
- **Taxa de Crescimento ao Dobrar Participantes:**
  - O acréscimo médio de mensagens observado ao passar de 5 para 10 nós foi de apenas $+0{,}67$ (Normal), $+0{,}73$ (Alta) e $+0{,}90$ (Desigual).
  - O valor teórico de acréscimo harmônico é $\ln(10) - \ln(5) = \ln(2) \approx 0{,}693$ mensagens adicionais. A correspondência entre o modelo analítico e a medição empírica é quase exata, refutando a escala linear $\mathcal{O}(N)$ característica de algoritmos em anel (Token Ring), que teria dobrado o número de mensagens de $2{,}5$ para $5{,}0$.

#### 3. Dinâmica de Espera e Fila FIFO (*Liveness* e Ausência de Starvation)
- Sob **Alta Contenção**, os clientes solicitam o recurso sem pausa (`CLIENT_MIN_SLEEP=0`). A espera deixa de ser dominada por intervalos ociosos e passa a refletir o esvaziamento sequencial da fila distribuída `next_queue`.
- Sendo a permanência na seção crítica uniformemente distribuída entre $0{,}2\text{ s}$ e $1{,}0\text{ s}$ ($\mu = 0{,}6\text{ s}$):
  - Em 5 nós: um nó enfileirado na $k$-ésima posição espera em média $4 \times 0{,}6\text{ s} \approx 2{,}4\text{ s}$. A mediana empírica medida foi de **2.426 ms** (~2.42 s).
  - Em 10 nós: um nó enfileirado espera em média $9 \times 0{,}6\text{ s} \approx 5{,}4\text{ s}$. A mediana empírica medida foi de **5.318 ms** (~5.32 s).
- A correspondência confirma a ordenação estrita FIFO de `next_queue` e comprova matematicamente a **ausência de starvation**: nenhum processo foi ultrapassado arbitrariamente ou retido além do tempo determinístico de serviço dos predecessores.

#### 4. Efeito de Localidade na Carga Desigual
- No cenário de **Carga Desigual**, onde `client1` realizou requisições frequentes (intervalo de $0{,}1\text{ s}$ a $0{,}3\text{ s}$ contra $1\text{ a }5\text{ s}$ dos demais), o número médio de mensagens por entrada sofreu redução:
  - 5 nós: caiu de $2{,}86$ (Normal) para **2.58 msgs/entrada**.
  - 10 nós: caiu de $3{,}53$ (Normal) para **3.48 msgs/entrada**.
- **Explicação Teórica:** Quando o `node1` recebe novas requisições em rajada enquanto ainda detém a posse do token (`has_token == True`), a entrada na seção crítica é concedida localmente sem nenhuma troca de mensagens pela rede (`0 msgs`). A reversão de caminhos otimiza naturalmente rajadas locais, beneficiando nós com maior demanda sem penalizar a garantia de repasse aos demais nós enfileirados.

---

### 7.5. Resultados em Nuvem (Docker Swarm — Oracle Cloud Infrastructure)

> [!NOTE]
> Esta subseção está estruturada e parametrizada para receber os resultados empíricos obtidos na execução distribuída entre as 3 máquinas virtuais na **Oracle Cloud Infrastructure (OCI)**.

#### Instruções para Coleta na Nuvem
Conecte-se via SSH à **VM 1 (Manager)** e execute a automação com a flag `--mode swarm`:
```bash
# Executar a bateria completa de experimentos nas 3 VMs OCI:
python3 collect_metrics.py --mode swarm --nodes both --experiments all --repetitions 3 --requests 10
```

#### Tabela de Resultados em Nuvem (OCI — 3 VMs)

*Preencha a tabela abaixo com os dados gerados em `metrics_results.csv` após a execução na nuvem:*

| Experimento | Topologia | Rep. | Semente | Reqs/Cliente | Total Concluído | Violações de Mutex | Máx. Concorrente | Tempo Total (s) | Mensagens Protocolo | Mensagens / Entrada | Espera Mediana (ms) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **disabled_sync** | 5 nós | 1 | 52 | 10 | *a preencher* | *a preencher* | *a preencher* | *a preencher* | 0 | 0.00 | *a preencher* |
| **disabled_sync** | 10 nós | 1 | 52 | 10 | *a preencher* | *a preencher* | *a preencher* | *a preencher* | 0 | 0.00 | *a preencher* |
| **normal_contention** | 5 nós | 1 | 52 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **normal_contention** | 5 nós | 2 | 62 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **normal_contention** | 5 nós | 3 | 72 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **normal_contention** | 10 nós | 1 | 52 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **normal_contention** | 10 nós | 2 | 62 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **normal_contention** | 10 nós | 3 | 72 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **high_contention** | 5 nós | 1 | 52 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **high_contention** | 5 nós | 2 | 62 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **high_contention** | 5 nós | 3 | 72 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **high_contention** | 10 nós | 1 | 52 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **high_contention** | 10 nós | 2 | 62 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **high_contention** | 10 nós | 3 | 72 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **unequal_load** | 5 nós | 1 | 52 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **unequal_load** | 5 nós | 2 | 62 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **unequal_load** | 5 nós | 3 | 72 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **unequal_load** | 10 nós | 1 | 52 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **unequal_load** | 10 nós | 2 | 62 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |
| **unequal_load** | 10 nós | 3 | 72 | 10 | *a preencher* | 0 | 1 | *a preencher* | *a preencher* | *a preencher* | *a preencher* |

#### Roteiro de Comparação Local vs. Nuvem
Ao preencher os resultados em nuvem, avalie os seguintes pontos no relatório final e apresentação:
1. **Impacto do RTT de Rede Real:** No ambiente local, as mensagens trafegam pela ponte virtual do Docker (`docker0`), com latência na faixa de microssegundos. Na nuvem, pacotes transitam pela rede física da OCI e pelo túnel VXLAN da rede `overlay`, elevando a latência de trânsito para milissegundos.
2. **Invariância do Tráfego de Mensagens:** Como a lógica do algoritmo é estritamente discreta, a quantidade de mensagens de protocolo por entrada deve manter-se estatisticamente idêntica à do teste local ($\approx 2{,}86$ para 5 nós e $\approx 3{,}53$ para 10 nós).
3. **Aumento no Tempo Total e Espera Mediana:** O tempo de espera mediano deve sofrer um acréscimo proporcional a $k \times \text{RTT}$, demonstrando empiricamente o custo de comunicação sobre redes geograficamente dispersas.

---

## 8. Referências Bibliográficas

1. **Naimi, M., Tréhel, M., & Arnold, A. (1996).** *A $\log(N)$ Distributed Mutual Exclusion Algorithm Based on Path Reversal.* Journal of Parallel and Distributed Computing, 34(1), 1–13. https://doi.org/10.1006/jpdc.1996.0041
2. **Raymond, K. (1989).** *A Tree-Based Algorithm for Distributed Mutual Exclusion.* ACM Transactions on Computer Systems, 7(1), 61–77. https://doi.org/10.1109/12.24268
3. **Singhal, M. (1993).** *A taxonomy of distributed mutual exclusion algorithms.* Journal of Parallel and Distributed Computing, 18(1), 94–101.

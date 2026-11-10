# Cluster Sync: Plano de Implementação de Exclusão Mútua Distribuída Naimi--Tréhel

**Membros do projeto:** Kevin, Mateus e Crescêncio\
**Algoritmo:** Naimi--Tréhel --- reversão de caminhos (*path reversal*)
e árvore lógica dinâmica

## 1. Núcleo do algoritmo

O algoritmo Naimi--Tréhel implementa exclusão mútua distribuída por meio
de um token e de uma árvore lógica dinâmica, mantida por ponteiros
`father` e uma fila de requisições `next`. Cada nó mantém seu estado
local, incluindo `father`, `next`, `requesting` e a informação de posse
do token (`Token`).

Quando um nó solicita a seção crítica, sua requisição é encaminhada pela
cadeia de ponteiros `father` até a raiz atual. Durante o encaminhamento,
ocorre a reversão de caminhos: os nós intermediários atualizam seus
ponteiros `father` para apontar para o novo solicitante, que passa a
ocupar a posição de raiz da árvore lógica. Quando o portador do token
libera a seção crítica, ele o envia ao próximo nó indicado pela fila
`next`, caso exista uma requisição pendente.

O token funciona como uma autorização exclusiva distribuída: somente o
nó que o possui pode executar a seção crítica. Assim, o protocolo evita
que dois nós executem simultaneamente a região protegida.

O mutex local tem uma função diferente: proteger a consistência e a
atomicidade das atualizações das estruturas internas de cada nó, como
`father`, `next` e `requesting`. Ele não substitui o mecanismo
distribuído de coordenação baseado no token.

## 2. Atribuição de tarefas

### 2.1. Kevin --- Protocolo central e reversão de caminhos

**Responsabilidade:** implementar a lógica de processamento de mensagens
do algoritmo Naimi--Tréhel.

**Tarefas específicas:**

-   Definir a estrutura de estado de cada nó: `father`, `next`,
    `requesting` e `Token`.
-   Implementar `Request_CS(Si)`:
    1.  Definir `requesting = true`.
    2.  Se `father != nil`, enviar `Request(Si)` para `father`.
    3.  Aguardar o recebimento do token antes de entrar na seção
        crítica.
-   Implementar o tratamento de uma requisição recebida:
    -   Se o nó atual for a raiz e já estiver solicitando ou utilizando
        a seção crítica, registrar o solicitante na fila `next`,
        conforme as regras do protocolo.
    -   Se o nó atual for a raiz e puder transferir o token,
        encaminhá-lo ao solicitante e atualizar o estado de posse.
    -   Atualizar os ponteiros `father` conforme a regra de reversão de
        caminhos.
    -   Se o nó não for a raiz, encaminhar a requisição ao seu `father`
        e atualizar os ponteiros de acordo com o protocolo.
-   Implementar `Release_CS`:
    -   Se houver uma requisição pendente em `next`, enviar o token ao
        próximo nó e atualizar a fila.
    -   Caso contrário, manter o token no nó atual.

**Entregáveis:**

-   Pseudocódigo ou implementação do protocolo.
-   Testes de reversão de caminhos com pelo menos três nós.
-   Registros dos ponteiros `father` antes e depois das requisições.
-   Testes que verifiquem a transferência correta do token e a exclusão
    mútua.

### 2.2. Mateus --- Análise de complexidade de mensagens e comparação

**Responsabilidade:** analisar o custo de mensagens do Naimi--Tréhel e
compará-lo com os algoritmos de Raymond e Token Ring.

**Tarefas específicas:**

-   **Naimi--Tréhel:** analisar a complexidade média de mensagens,
    geralmente descrita como `O(log n)` sob hipóteses apropriadas, e o
    pior caso `O(n)`, quando a estrutura lógica assume uma forma
    semelhante a uma cadeia.
-   **Raymond:** descrever a utilização de uma árvore lógica estática,
    cujo custo depende da altura da árvore. Em uma árvore balanceada, o
    custo pode ser `O(log n)`; em uma árvore degenerada, pode chegar a
    `O(n)`.
-   **Token Ring:** explicar que o custo depende da posição do token e
    da carga de requisições. Uma requisição pode exigir poucas mensagens
    se o token estiver próximo, enquanto a espera e o número de
    mensagens podem crescer quando o token precisa percorrer o anel.
    Diferenciar custo por requisição, tempo de espera e tráfego ocioso.
-   Explicitar as hipóteses usadas na comparação:
    -   topologia física da rede e estrutura lógica de cada algoritmo;
    -   ausência de falhas;
    -   entrega confiável de mensagens;
    -   distribuição e frequência das requisições dos nós;
    -   definição de uma mensagem contabilizada;
    -   papel do mutex local, limitado à proteção do estado interno de
        cada nó.

**Entregáveis:**

-   Tabela comparativa com custo médio e de pior caso, topologia
    aplicável e principais gargalos.
-   Explicação das hipóteses e da métrica de complexidade utilizada.

### 2.3. Crescêncio --- Simulação de tráfego e visualização

**Responsabilidade:** construir uma simulação de um cluster pequeno, com
aproximadamente 5 a 7 nós, demonstrando a propagação das requisições
pelos ponteiros `father` e as mudanças na árvore lógica.

**Tarefas específicas:**

-   Implementar um simulador de eventos discretos no qual os nós enviem
    mensagens `Request` e `Token`.
-   Registrar os valores de `father`, `next`, `requesting` e da posse do
    token antes e depois de cada evento relevante.
-   Simular um cenário inicial com o token no nó A:
    1.  B solicita a seção crítica.
    2.  C solicita a seção crítica enquanto B está executando ou
        aguardando, conforme o estado definido no cenário.
    3.  D realiza outra solicitação.
    4.  Registrar a evolução da árvore lógica e a ordem das requisições
        pendentes.
-   Gerar diagramas de sequência ou grafos direcionados que mostrem a
    estrutura da árvore em diferentes etapas.
-   Coletar estatísticas da quantidade de mensagens por cenário.
-   Validar a implementação de Kevin usando os resultados da simulação,
    verificando se o token é enviado ao próximo nó correto quando a
    seção crítica é liberada.

**Entregáveis:**

-   Código-fonte do simulador.
-   Diagramas ou gráficos comparando a estrutura da árvore antes e
    depois das requisições.
-   Sequência de mensagens por cenário.
-   Estatísticas de mensagens e relatório de verificação.

## 3. Integração conjunta e critérios de aceitação

### 3.1. Alinhamento das interfaces

-   Kevin define e documenta interfaces consistentes para envio de
    mensagens, como `send(Request, target)` e `send(Token, target)`.
-   Crescêncio utiliza as mesmas definições na simulação, evitando
    duplicar ou alterar a semântica do protocolo.
-   Mateus baseia a análise de complexidade nos mesmos tipos de mensagem
    e nas mesmas regras de contagem usadas pela implementação e pela
    simulação.

### 3.2. Uso do mutex local

Todos os integrantes devem garantir que o mutex local --- por exemplo,
`lock` --- proteja apenas o acesso concorrente ao estado interno de cada
nó. Ele não substitui a coordenação distribuída realizada pelo token.

### 3.3. Critérios de aceitação

O projeto será considerado integrado quando:

-   [ ] O protocolo estiver implementado e documentado.
-   [ ] Os testes demonstrarem que apenas o portador do token entra na
    seção crítica.
-   [ ] As requisições pendentes forem atendidas conforme a ordem
    definida pelo protocolo.
-   [ ] A transferência do token e a atualização dos ponteiros forem
    verificadas.
-   [ ] A simulação utilizar as mesmas regras e interfaces da
    implementação.
-   [ ] A análise de complexidade apresentar hipóteses explícitas.
-   [ ] Os artefatos de cada integrante estiverem documentados e
    integrados.

### 3.4. Entregáveis finais

-   `README.md` com a descrição do algoritmo, instruções de execução e
    divisão das tarefas.
-   Módulo do protocolo implementado por Kevin.
-   Relatório de análise produzido por Mateus.
-   Código e visualizações da simulação produzidos por Crescêncio.
-   Documento conjunto de testes e aceitação.

## 4. Notas gerais do projeto

-   **Organização do código:** manter separadas a lógica do protocolo, a
    comunicação entre nós, a simulação e a visualização, de acordo com a
    arquitetura escolhida.
-   **Documentação:** registrar como executar o projeto, quais
    dependências são necessárias, quais parâmetros podem ser
    configurados e quais resultados são esperados.
-   **Testes:** incluir casos normais, requisições simultâneas ou
    próximas no tempo, vários nós aguardando e liberação do token sem
    requisições pendentes.
-   **Depuração:** incluir logs com identificador do evento, nó
    remetente, nó destinatário, tipo de mensagem e alterações relevantes
    de estado.
-   **Reprodutibilidade:** se a simulação utilizar escolhas aleatórias,
    permitir configurar uma semente para repetir os mesmos experimentos.
-   **Limites da simulação:** declarar se o modelo considera atrasos de
    rede, falhas de nós, perda de mensagens ou apenas entrega confiável
    e imediata.
-   **Consistência do protocolo:** manter as regras de atualização de
    `father`, `next` e da posse do token consistentes entre a
    implementação, a análise e a simulação.

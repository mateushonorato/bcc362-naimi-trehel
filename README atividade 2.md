# README — Naimi–Tréhel: Como a Solução Resolve as Questões do TP2

**Projeto:** Cluster Sync — Exclusão Mútua Distribuída  
**Equipe:** Kevin, Mateus e Crescêncio  
**Algoritmo:** Naimi–Tréhel (token + árvore lógica dinâmica com reversão de caminhos)

---

## 1. Como o Naimi–Tréhel atende às quatro propriedades? (Slide 8)

| Propriedade | Como o algoritmo garante |
|---|---|
| **Exclusão mútua** | Existe **um único token** no sistema. Somente o nó que possui o token pode entrar na seção crítica. A reversão de caminhos reorganiza os ponteiros `father` para entregar o token, mas nunca o duplica. |
| **Ausência de deadlock** | A estrutura é uma **árvore acíclica**. Os pedidos sobem em direção à raiz, onde está o token. Assim, não há espera circular possível. |
| **Ausência de starvation** | Cada nó mantém uma **fila FIFO (`next`)**. Ao sair da seção crítica, o portador do token o entrega ao primeiro nó da fila. Dessa forma, ninguém ultrapassa os pedidos anteriores. |
| **Ordenação** | A ordem é **posicional**, definida pela chegada dos pedidos à raiz/árvore, e não por um relógio lógico global. É uma ordenação justa, mas não necessariamente cronológica. |

## 2. Classificação na taxonomia de Singhal (Slide 11)

> **Família: algoritmos baseados em token** — “entro quando o objeto está comigo”.

- É um **sucessor do algoritmo de Raymond (1989)**, mas apresenta uma diferença fundamental: a árvore não é fixa.
- A cada pedido, a árvore é reorganizada por meio da **reversão de caminho**, tornando o solicitante a nova raiz.
- Nessa família, o direito de entrar na seção crítica pertence ao nó que possui o token; a fila `next` determina a ordem de entrega.

## 3. Custo teórico e prático (Slide 15)

| Cenário | Custo em mensagens |
|---|---|
| **Caso médio (árvore balanceada)** | `O(log N)` — resultado esperado sob as hipóteses do artigo original. |
| **Pior caso (topologia em cadeia)** | `O(N)` — o pedido pode percorrer toda a cadeia até alcançar o token. |
| **Após a reversão de caminho** | A árvore tende a ficar mais achatada, o que pode reduzir os custos dos pedidos seguintes. |

**Hipótese da análise:** pedidos aleatórios e uma árvore razoavelmente balanceada. Uma comparação com o algoritmo de Raymond só é justa quando a mesma topologia inicial é considerada.

## 4. Hipóteses declaradas (Slide 16)

| Hipótese | Nossa solução |
|---|---|
| **Canais FIFO?** | Sim. As mensagens entre dois nós chegam na mesma ordem em que foram enviadas. |
| **Entrega confiável?** | Sim. O escopo do TP2 considera a operação normal, sem perda de mensagens. |
| **Número de nós conhecido e fixo?** | Sim. Os nós conhecem seus vizinhos por meio da referência `father`. |
| **Ausência de falhas?** | Sim. Considera-se que o token não se perde e que nenhum nó falha. |
| **Ordenação de pedidos no mesmo nó?** | Sim. Um **mutex local** serializa os pedidos internos antes de entrarem no protocolo. |

> **Atenção: mutex local ≠ protocolo distribuído.**  
> O mutex local protege apenas a estrutura interna de cada nó (`father`, `next` e `requesting`). A coordenação da exclusão mútua entre os diferentes nós é realizada pelo **token**.

## 5. Divisão do trabalho

- **Kevin:** protocolo central e reversão de caminhos — implementação e testes.
- **Mateus:** análise de custo e comparação com Raymond/Token Ring — relatório e hipóteses.
- **Crescêncio:** simulação e visualização da árvore dinâmica — diagramas e verificação.

## 6. Como apresentar no seminário

1. **Kevin:** demonstrar como a reversão de caminho trata o custo do algoritmo de Raymond e evita o gargalo de um coordenador central.
2. **Mateus:** utilizar o Slide 15 para justificar o custo médio `O(log N)` e destacar o pior caso `O(N)`, relacionando-o às hipóteses do Slide 16.
3. **Crescêncio:** utilizar diagramas ou animações para demonstrar visualmente a exclusão mútua e a ausência de starvation.

---

*Documento de apoio do projeto Cluster Sync — TP2.*

# MPComm — Multiparty Communication

## Sobre o projeto

O MPComm é uma aplicação desenvolvida em Python que demonstra a comunicação entre múltiplos processos em um sistema distribuído.

Nesta etapa do trabalho, foram realizadas melhorias nos protocolos de comunicação, divididas em duas partes:

- **Parte 1(a):** substituição das interações cliente-servidor que utilizavam sockets TCP diretamente por chamadas RPC, utilizando XML-RPC.
- **Parte 1(b):** implementação de mecanismos para tratamento de perda de mensagens e ordenação FIFO na comunicação entre os peers, mantendo a utilização de sockets UDP.

## Integrante

| Nome | Matrícula | E-mail |
|---|---|---|
| Maria Michelle Rodrigues Amaral | 202401809 | mariaamaral@discente.com |

## Funcionalidades

- Registro e gerenciamento dos peers participantes.
- Inicialização remota dos peers por XML-RPC.
- Comunicação cliente-servidor utilizando chamadas RPC.
- Troca de mensagens entre os peers utilizando UDP.
- Realização de handshake antes do envio das mensagens.
- Confirmação de recebimento por meio de ACK.
- Retransmissão de mensagens sem confirmação.
- Controle de sequência e armazenamento temporário de mensagens recebidas fora de ordem.
- Geração de arquivos de log.
- Envio dos registros ao servidor de comparação.
- Verificação de violações de ordenação FIFO, mensagens ausentes e mensagens inválidas.
- Encerramento controlado da aplicação.

## Arquitetura

A aplicação possui três componentes principais:

| Componente | Responsabilidade |
|---|---|
| `GroupMngr.py` | Gerencia o registro e a lista de peers participantes. |
| `comparisonServer.py` | Coordena a execução dos peers, recebe os registros e verifica a ordenação das mensagens. |
| `peerCommunicatorUDP.py` | Executa os peers e realiza a comunicação entre eles. |

### Comunicação entre os componentes

```text
                 Comparison Server
                   /           \
              XML-RPC         XML-RPC
                /                 \
        Group Manager           Peers
                                  |
                                 UDP
                                  |
                         Troca de mensagens
                           entre os peers
```

O XML-RPC é utilizado nas operações de gerenciamento, inicialização e envio dos registros ao servidor. Já o UDP permanece responsável pela troca direta de mensagens entre os peers.

## Detalhes da implementação

### Parte 1(a) — Substituição de sockets por RPC

Originalmente, as interações cliente-servidor utilizavam sockets TCP diretamente. Para essa etapa, a comunicação foi substituída por chamadas RPC, utilizando XML-RPC.

O XML-RPC permite que um processo execute métodos disponibilizados remotamente por outro processo. Dessa forma, as operações entre os componentes passam a ser realizadas por chamadas de métodos, abstraindo parte dos detalhes de comunicação que precisariam ser tratados manualmente com sockets.

**Comparação entre sockets TCP e XML-RPC**

| Critério | Sockets TCP | XML-RPC |
|---|---|---|
| Abstração | Comunicação realizada por envio e recebimento de dados. | Comunicação realizada por chamadas de métodos remotos. |
| Implementação | Exige tratamento direto dos detalhes da comunicação. | Simplifica a interação entre cliente e servidor. |
| Controle | Oferece maior controle sobre a comunicação. | Parte dos detalhes é gerenciada pela biblioteca. |
| Flexibilidade | Permite definir livremente o formato das mensagens. | Utiliza uma estrutura padronizada de requisições e respostas XML. |
| Sobrecarga | Menor sobrecarga de abstração. | Possui sobrecarga adicional devido ao HTTP, XML e serialização. |

**Vantagens do XML-RPC:**
- Simplifica a comunicação entre processos.
- Permite executar métodos remotamente sem implementar manualmente todo o protocolo de troca de mensagens.
- Facilita a organização e manutenção do código.

**Desvantagens do XML-RPC:**
- Introduz sobrecarga devido à utilização de HTTP e XML.
- Oferece menos controle sobre os detalhes de transporte.
- Depende de uma interface de métodos e dos tipos de dados suportados pelo protocolo.

Assim, o XML-RPC torna as interações cliente-servidor mais simples, enquanto os sockets oferecem maior controle sobre a comunicação. A escolha depende das necessidades da aplicação.

### Parte 1(b) — Tratamento de perda de mensagens e ordenação FIFO

Nesta etapa, os sockets UDP foram mantidos na comunicação entre os peers. Como o UDP não garante a entrega nem a ordenação das mensagens, foram implementados mecanismos adicionais para aumentar a confiabilidade da comunicação.

**Tratamento de perda de mensagens**

Foi implementado um mecanismo de confirmação de recebimento utilizando ACK (*Acknowledgment*).

O funcionamento ocorre da seguinte forma:

1. O peer remetente envia uma mensagem ao destinatário.
2. Ao receber a mensagem, o destinatário envia uma confirmação (ACK).
3. Caso o remetente não receba a confirmação dentro do intervalo de espera definido, realiza uma nova tentativa de envio.
4. O processo de retransmissão é limitado a uma quantidade máxima de tentativas.

As mensagens recebidas também são identificadas por seus números de sequência, permitindo reconhecer mensagens duplicadas e evitar que sejam entregues mais de uma vez.

**Ordenação FIFO**

Para implementar a ordenação FIFO (*First In, First Out*), cada mensagem possui um número de sequência associado ao seu remetente.

O receptor mantém o controle da próxima sequência esperada. Quando uma mensagem chega fora de ordem, ela é armazenada temporariamente em um buffer até que as mensagens anteriores sejam recebidas.

Quando a sequência esperada é recebida, o receptor entrega a mensagem e verifica se existem outras mensagens armazenadas que já podem ser processadas.

Dessa forma, as mensagens são registradas respeitando a ordem de envio de cada remetente.

**Verificação dos registros**

Ao final da comunicação, os registros dos peers são enviados ao `comparisonServer.py`, responsável por verificar:

- Violações da ordenação FIFO.
- Mensagens ausentes.
- Mensagens inválidas.

Essa verificação permite avaliar se as mensagens foram recebidas e registradas conforme a sequência esperada.

## Tecnologias e bibliotecas utilizadas

A implementação foi desenvolvida em Python, utilizando recursos da biblioteca padrão:

- **XML-RPC (`xmlrpc.server` e `xmlrpc.client`):** disponibilização e execução de métodos remotamente.
- **Sockets UDP (`socket`):** envio e recebimento de mensagens entre os peers.
- **Threads (`threading`):** execução simultânea dos servidores e das atividades de comunicação.
- **Filas (`queue`):** organização das solicitações e dos registros recebidos.
- **Pickle (`pickle`):** serialização e desserialização das mensagens transmitidas por UDP.

## Como executar

### Requisitos

- Python 3 instalado.
- Git.

Não são necessárias bibliotecas externas para a versão apresentada, pois os recursos utilizados pertencem à biblioteca padrão do Python.

### 1. Obter o projeto

```bash
git clone https://github.com/mrmichelledev/SD-Parte1.git
cd SD-Parte1/MPComm
```

### 2. Iniciar a aplicação

Abra três terminais na pasta `MPComm` e execute os componentes nesta ordem.

**Terminal 1 — Group Manager**

```bash
python GroupMngr.py
```

**Terminal 2 — Peer**

```bash
python peerCommunicatorUDP.py
```

**Terminal 3 — Comparison Server**

```bash
python comparisonServer.py
```

Quando solicitado pelo Comparison Server, informe a quantidade de mensagens que cada peer deverá enviar.

Para encerrar a aplicação, informe `0` no Comparison Server.

### Configurações

Os endereços, as portas e a quantidade de peers esperada são definidos em `constMP.py`.

A configuração utilizada nos testes locais utiliza `127.0.0.1` (*localhost*), permitindo executar os processos no mesmo computador.

## Testes realizados

Os testes foram realizados localmente, em uma única máquina, utilizando a configuração de um peer (`N = 1`) e três mensagens.

Foram avaliados os seguintes cenários:

**1. Comunicação básica**

- Registro do peer no Group Manager.
- Inicialização remota por XML-RPC.
- Realização do handshake.
- Envio e recebimento de mensagens UDP.
- Geração e envio dos registros ao servidor.
- Encerramento da aplicação.

**2. Simulação de perda de mensagem**

Foi simulada a perda de uma mensagem DATA para verificar o mecanismo de confirmação e retransmissão.

Após a nova tentativa de envio, as mensagens foram recebidas e registradas corretamente.

**3. Recebimento de mensagens fora de ordem**

Foi realizado um teste enviando as mensagens em sequência inversa, para verificar o funcionamento do buffer e do controle de sequência.

Apesar da ordem de chegada, o registro final apresentou as mensagens na sequência FIFO esperada.

**Resultado da verificação**

Nos testes realizados, o Comparison Server apresentou o seguinte resultado:

```text
FIFO verification:

Receiver 0: 3 messages, FIFO violations: 0, missing messages: 0, invalid messages: 0

Final result: 0 FIFO violations, 0 missing messages, 0 invalid messages.
```

Os resultados indicam que, nos cenários avaliados, não foram identificadas violações de FIFO, mensagens ausentes ou mensagens inválidas.

**Limitações dos testes**

Os testes foram realizados com apenas um peer, em uma única máquina. Portanto, não foi validada a comunicação entre máquinas diferentes nem a ordenação entre múltiplos participantes simultâneos.

## Considerações finais

As melhorias implementadas permitiram aprimorar os protocolos de comunicação da aplicação.

Na Parte 1(a), a utilização de XML-RPC simplificou as interações cliente-servidor, substituindo a comunicação direta por sockets TCP por chamadas de métodos remotos.

Na Parte 1(b), a comunicação UDP entre os peers foi mantida, com a implementação de confirmações, retransmissões e controle de sequência para tratar perdas de mensagens e garantir a ordenação FIFO.

Os testes realizados demonstraram o funcionamento dos mecanismos implementados nos cenários locais avaliados, contribuindo para uma comunicação mais confiável e organizada entre os processos.

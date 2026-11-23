# MPComm — Multiparty Communication

## Sobre o projeto

O MPComm é uma aplicação desenvolvida em Python que demonstra a comunicação entre múltiplos processos em um sistema distribuído.

Nesta etapa do trabalho, o objetivo foi substituir as comunicações cliente-servidor que utilizavam sockets TCP diretamente por chamadas RPC, utilizando **XML-RPC**, mantendo a comunicação UDP entre os peers.

## Integrantes

| Nome | Matrícula | E-mail |
|---|---|---|
| Maria Michelle Rodrigues Amaral | 202401809 | mariaamaral@discente.com |

## Funcionalidades

- Registro e gerenciamento dos peers participantes.
- Inicialização remota dos peers.
- Troca de mensagens entre os participantes utilizando UDP.
- Realização de handshake antes do envio das mensagens.
- Armazenamento das mensagens recebidas em arquivos de log.
- Envio dos registros ao servidor de comparação.
- Verificação de diferenças na ordem de recebimento das mensagens.
- Encerramento controlado da aplicação.

## Arquitetura

A aplicação possui três componentes principais:

| Componente | Responsabilidade |
|---|---|
| `GroupMngr.py` | Gerencia o registro e a lista de peers. |
| `comparisonServer.py` | Coordena a execução e compara os registros recebidos. |
| `peerCommunicatorUDP.py` | Executa os peers e realiza a troca de mensagens. |

### Comunicação entre os componentes

<escape>```text
             Comparison Server
                    |
              XML-RPC / TCP
                    |
               Group Manager
                    |
              XML-RPC / TCP
                    |
                   Peers
                    |
                  UDP
                    |
           Troca de mensagens
             entre os peers
```</escape>

O XML-RPC é utilizado nas operações de gerenciamento, inicialização e envio dos registros. Já o UDP permanece responsável pela comunicação direta entre os peers.

## Detalhes da implementação

A melhoria foi realizada utilizando recursos da biblioteca padrão do Python:

- **XML-RPC (`xmlrpc.server` e `xmlrpc.client`):** permite disponibilizar e executar métodos remotamente entre os processos.
- **Sockets UDP (`socket`):** responsáveis pelo envio e recebimento de mensagens entre os peers.
- **Threads (`threading`):** permitem executar simultaneamente os servidores e as atividades de comunicação.
- **Filas (`queue`):** organizam as solicitações de inicialização dos peers e os registros recebidos pelo servidor.
- **Pickle (`pickle`):** realiza a serialização e desserialização das mensagens transmitidas por UDP.

## Como executar

### Requisitos

- Python 3 instalado.
- Git.

Não são necessárias bibliotecas externas para esta versão, pois os recursos utilizados pertencem à biblioteca padrão do Python.

### 1. Obter o projeto

```bash
git clone https://github.com/mrmichelledev/SD-Parte1.git
cd SD-Parte1/MPComm
```

### 2. Iniciar a aplicação

Abra três terminais na pasta `MPComm` e execute os componentes nesta ordem:

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

Quando solicitado, informe a quantidade de mensagens que cada peer deverá enviar.

Para encerrar a aplicação, informe `0` no Comparison Server.

### Configurações

Os endereços, as portas e a quantidade de peers esperada são definidos em `constMP.py`.

A configuração atual utiliza `127.0.0.1` (localhost), permitindo executar os processos no mesmo computador.

## Testes realizados

Foi realizado um teste local com um peer e três mensagens.

Foram verificadas as seguintes etapas:

- Registro do peer no Group Manager.
- Inicialização remota por XML-RPC.
- Troca de mensagens e handshakes por UDP.
- Geração e envio dos arquivos de log.
- Comparação das mensagens recebidas.
- Encerramento da aplicação.

Resultado apresentado:

```text
Found 0 unordered message rounds
```

O resultado indica que não foram identificadas diferenças de ordenação no teste realizado. Como a execução utilizou apenas um peer, ainda não representa uma validação da ordenação entre múltiplos participantes.

## Considerações finais

A implementação permitiu substituir as interações cliente-servidor baseadas em sockets TCP diretamente por chamadas XML-RPC, simplificando a comunicação entre os componentes.

A comunicação UDP entre os peers foi preservada, mantendo a estrutura original de troca de mensagens da aplicação.

from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
from constMP import *
import threading
import queue


# Fila utilizada para armazenar os logs recebidos dos peers
# A função XML-RPC coloca os logs na fila e o servidor principal os processa
log_queue = queue.Queue()


# Função chamada remotamente pelos peers para enviar seus logs
def receive_log(logList):
    print('Received log from peer')

    # Armazena o log recebido para ser utilizado na comparação
    log_queue.put(logList)

    return True


# Cria o servidor XML-RPC responsável por receber os logs dos peers
server = SimpleXMLRPCServer(
    ('0.0.0.0', SERVER_PORT),
    allow_none=True
)

# Disponibiliza a função receive_log para chamadas remotas
server.register_function(receive_log, 'receive_log')

# Executa o servidor em uma thread separada para não bloquear o programa principal
threading.Thread(
    target=server.serve_forever,
    daemon=True
).start()


def mainLoop():
    while 1:
        nMsgs = promptUser()

        # Conecta ao Group Manager utilizando XML-RPC
        groupManager = ServerProxy(
            f'http://{GROUPMNGR_ADDR}:{GROUPMNGR_TCP_PORT}'
        )

        # Consulta os peers registrados no Group Manager
        peerList = groupManager.list_peers()

        print("List of Peers: ", peerList)

        # Envia o comando de inicialização para os peers
        startPeers(peerList, nMsgs)

        if nMsgs != 0:
            print('Now, wait for the message logs from the communicating peers...')

            waitForLogsAndCompare(nMsgs)

        else:
            print('Stopping.')

            # Envia o comando para encerrar o Group Manager
            groupManager.stop_server()

            # Encerra o servidor XML-RPC de recebimento dos logs
            server.shutdown()
            server.server_close()

            break


def promptUser():
    nMsgs = int(input(
        'Enter the number of messages for each peer to send (0 to terminate)=> '
    ))

    return nMsgs


def startPeers(peerList, nMsgs):
    # Percorre os peers registrados e envia o sinal de inicialização
    peerNumber = 0

    for peer in peerList:

        # Conecta ao servidor XML-RPC do peer
        client = ServerProxy(
            f'http://{peer}:{PEER_TCP_PORT}'
        )

        # Chama remotamente a função start_peer, enviando o identificador
        # do processo e a quantidade de mensagens que ele deverá enviar
        response = client.start_peer(peerNumber, nMsgs)

        print(response)

        peerNumber = peerNumber + 1


def waitForLogsAndCompare(N_MSGS):
    # Lista que armazenará os logs recebidos de todos os peers
    # Cada log contém as mensagens recebidas por um processo
    numPeers = 0
    msgs = []

    # Aguarda receber os logs de todos os peers através da fila
    while numPeers < N:
        logList = log_queue.get()

        msgs.append(logList)
        numPeers = numPeers + 1

    unordered = 0

    # Compara as mensagens recebidas pelos diferentes processos
    for j in range(0, N_MSGS):

        firstMsg = msgs[0][j]

        for i in range(1, N):

            if firstMsg != msgs[i][j]:
                unordered = unordered + 1
                break

    print('Found ' + str(unordered) + ' unordered message rounds')


# Inicia o servidor principal
mainLoop()

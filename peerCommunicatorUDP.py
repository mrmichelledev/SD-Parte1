
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
from socket import *
from constMP import *
import threading
import random
import time
import pickle
import queue

# Contador para verificar se recebemos os handshakes de todos os processos
handShakeCount = 0

PEERS = []

# Fila usada para receber os comandos de inicialização enviados pelo servidor
# Assim, a comunicação XML-RPC pode acontecer em outra thread sem bloquear o programa principal
start_queue = queue.Queue()


# Função chamada remotamente pelo comparisonServer para iniciar este peer
def start_peer(peer_number, n_msgs):
    # Coloca os parâmetros recebidos na fila para o programa principal processar
    start_queue.put((peer_number, n_msgs))
    return f'Peer process {peer_number} started.'


# UDP sockets to send and receive data messages:
# Socket utilizado para enviar mensagens UDP
sendSocket = socket(AF_INET, SOCK_DGRAM)

# Socket utilizado para receber mensagens UDP
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))


def get_public_ip():
    # Endereço local utilizado nos testes executados na mesma máquina
    ipAddr = '127.0.0.1'
    print('My local IP address is: {}'.format(ipAddr))
    return ipAddr


# Registra este peer no Group Manager utilizando XML-RPC
def registerWithGroupManager():
    groupManager = ServerProxy(
        f'http://{GROUPMNGR_ADDR}:{GROUPMNGR_TCP_PORT}'
    )

    ipAddr = get_public_ip()

    print('Registering with group manager:', ipAddr)

    # Envia o endereço e a porta UDP deste peer para o Group Manager
    groupManager.register_peer(ipAddr, PEER_UDP_PORT)

# Consulta a lista de peers registrados no Group Manager utilizando XML-RPC
def getListOfPeers():
    groupManager = ServerProxy(
        f'http://{GROUPMNGR_ADDR}:{GROUPMNGR_TCP_PORT}'
    )

    print('Getting list of peers from group manager...')

    peers = groupManager.list_peers()

    print('Got list of peers:', peers)

    return peers


class MsgHandler(threading.Thread):

    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')

        global handShakeCount

        logList = []

        # Aguarda receber os handshakes de todos os processos antes de iniciar
        # o recebimento das mensagens
        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)

            if msg[0] == 'READY':

                # To do: send reply of handshake and wait for confirmation

                handShakeCount = handShakeCount + 1
                print('--- Handshake received: ', msg[1])

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0

        while True:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)

            if msg[0] == -1:
                # Conta as mensagens de encerramento recebidas
                stopCount = stopCount + 1

                if stopCount == N:
                    break
            else:
                print('Message ' + str(msg[1]) + ' from process ' + str(msg[0]))
                logList.append(msg)

        # Salva as mensagens recebidas em um arquivo de log
        logFile = open('logfile' + str(myself) + '.log', 'w')
        logFile.writelines(str(logList))
        logFile.close()

        # Envia a lista de mensagens ao servidor para comparação utilizando XML-RPC
        print('Sending the list of messages to the server for comparison...')

        client = ServerProxy(
            f'http://{SERVER_ADDR}:{SERVER_PORT}'
        )

        client.receive_log(logList)

        # Reinicia o contador de handshakes para uma próxima execução
        handShakeCount = 0


# Cria o servidor XML-RPC deste peer para receber comandos de inicialização
peerServer = SimpleXMLRPCServer(
    ('0.0.0.0', PEER_TCP_PORT),
    allow_none=True
)

# Disponibiliza a função start_peer para chamadas remotas
peerServer.register_function(start_peer, 'start_peer')

# Executa o servidor XML-RPC em uma thread separada para não bloquear o programa principal
threading.Thread(
    target=peerServer.serve_forever,
    daemon=True
).start()


# From here, code is executed when program starts:

# Registra o peer no Group Manager
registerWithGroupManager()

while 1:
    print('Waiting for signal to start...')

    # Aguarda um comando de inicialização recebido pela função XML-RPC
    myself, nMsgs = start_queue.get()

    print('I am up, and my ID is: ', str(myself))

    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    # Cria a thread responsável por receber as mensagens UDP
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')

    PEERS = getListOfPeers()

    # Send handshakes
    # To do: Must continue sending until it gets a reply from each process
    #        Send confirmation of reply
    for addrToSend in PEERS:
        print('Sending handshake to ', addrToSend)
        msg = ('READY', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

    while (handShakeCount < N):
        pass  # find a better way to wait for the handshakes

    # Send a sequence of data messages to all other processes
    for msgNumber in range(0, nMsgs):

        # Aguarda um intervalo aleatório entre o envio das mensagens
        time.sleep(random.randrange(10, 100) / 1000)

        msg = (myself, msgNumber)
        msgPack = pickle.dumps(msg)

        for addrToSend in PEERS:
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
            print('Sent message ' + str(msgNumber))

    # Envia uma mensagem de encerramento para todos os processos
    for addrToSend in PEERS:
        msg = (-1, -1)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

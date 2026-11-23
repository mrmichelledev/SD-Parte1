from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
from socket import AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, socket, timeout
from constMP import *
import threading
import time
import pickle
import queue

# Tempo maximo de espera por uma confirmacao (ACK).
ACK_TIMEOUT = 0.5

# Quantidade de retransmissoes permitidas depois do envio inicial.
MAX_RETRIES = 8

# Tempo adicional em que o receptor continua ativo para responder
# a retransmissoes duplicadas que possam chegar no final da rodada.
LINGER_TIME = ACK_TIMEOUT * (MAX_RETRIES + 2)

# Fila de comandos XML-RPC enviados pelo Comparison Server.
start_queue = queue.Queue()


def start_peer(peer_number, n_msgs):
    """Recebe uma solicitacao XML-RPC para iniciar este peer."""
    start_queue.put((peer_number, n_msgs))
    return f'Peer process {peer_number} started.'


# Um unico socket UDP e utilizado para enviar e receber mensagens.
# Assim, as respostas ACK chegam na mesma porta em que o peer recebe dados.
udpSocket = socket(AF_INET, SOCK_DGRAM)
udpSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
udpSocket.bind(('0.0.0.0', PEER_UDP_PORT))


def get_public_ip():
    """Retorna localhost para os testes executados no mesmo computador."""
    ipAddr = '127.0.0.1'
    print('My local IP address is: {}'.format(ipAddr))
    return ipAddr


def registerWithGroupManager():
    """Registra este peer no Group Manager por XML-RPC."""
    groupManager = ServerProxy(
        f'http://{GROUPMNGR_ADDR}:{GROUPMNGR_TCP_PORT}'
    )

    ipAddr = get_public_ip()
    print('Registering with group manager:', ipAddr)
    groupManager.register_peer(ipAddr, PEER_UDP_PORT)


def getListOfPeers():
    """Consulta a lista de peers registrados no Group Manager."""
    groupManager = ServerProxy(
        f'http://{GROUPMNGR_ADDR}:{GROUPMNGR_TCP_PORT}'
    )

    print('Getting list of peers from group manager...')
    peers = groupManager.list_peers()
    print('Got list of peers:', peers)
    return peers


class MsgHandler(threading.Thread):
    """
    Recebe mensagens UDP, envia ACKs e entrega mensagens em ordem FIFO.

    Cada remetente possui:
    - proximo_numero: proxima sequencia que pode ser entregue;
    - buffer: mensagens recebidas antes da sequencia esperada.
    """

    def __init__(self, sock, peer_count, peer_id):
        super().__init__()
        self.sock = sock
        self.peer_count = peer_count
        self.peer_id = peer_id

        self.logList = []

        self.next_sequence = {i: 0 for i in range(peer_count)}
        self.buffers = {i: {} for i in range(peer_count)}
        self.end_totals = {}
        self.completed_senders = set()
        
        """
        # Controle temporario para simular a perda de uma mensagem no teste.
        self.dropped_test_packet = False
        
        # Controle temporario para simular a perda de um ACK.
        self.dropped_test_ack = False
        """
        
        # Conjuntos compartilhados com a thread principal.
        self.ready_acks = set()
        self.message_acks = set()
        self.end_acks = set()
        self.ack_condition = threading.Condition()

    def _send_packet(self, packet, address):
        self.sock.sendto(pickle.dumps(packet), address)

    def _check_sender_finished(self, sender_id):
        """Verifica se todas as mensagens de um remetente foram entregues."""
        if sender_id not in self.end_totals:
            return

        expected_total = self.end_totals[sender_id]
        if self.next_sequence[sender_id] >= expected_total:
            self.completed_senders.add(sender_id)

    def _handle_data(self, sender_id, sequence, address):
        # Confirma o recebimento mesmo se a mensagem for duplicada.
        # Isso permite que o remetente pare de retransmiti-la.
        self._send_packet(
            ('ACK', sender_id, self.peer_id, sequence),
            address
        )

        if sender_id not in self.next_sequence:
            return

        expected = self.next_sequence[sender_id]

        # Mensagens antigas ja foram entregues; nao devem ser duplicadas no log.
        if sequence < expected:
            return

        # Guarda a mensagem. Se ela chegou fora de ordem, fica no buffer.
        self.buffers[sender_id][sequence] = (sender_id, sequence)

        # Entrega somente a sequencia esperada e as proximas que ja estiverem
        # disponiveis no buffer.
        while expected in self.buffers[sender_id]:
            self.logList.append(self.buffers[sender_id].pop(expected))
            expected += 1

        self.next_sequence[sender_id] = expected
        self._check_sender_finished(sender_id)

    def _handle_packet(self, packet, address):
        message_type = packet[0]

        if message_type == 'READY':
            sender_id = packet[1]
            self._send_packet(
                ('READY_ACK', sender_id, self.peer_id),
                address
            )

        elif message_type == 'READY_ACK':
            target_sender, receiver_id = packet[1], packet[2]
            if target_sender == self.peer_id:
                with self.ack_condition:
                    self.ready_acks.add(receiver_id)
                    self.ack_condition.notify_all()

        elif message_type == 'DATA':
            sender_id, sequence = packet[1], packet[2]

            self._handle_data(sender_id, sequence, address)

        elif message_type == 'ACK':
            target_sender, receiver_id, sequence = packet[1], packet[2], packet[3]
            if target_sender == self.peer_id:
                with self.ack_condition:
                    self.message_acks.add((sequence, receiver_id))
                    self.ack_condition.notify_all()

        elif message_type == 'END':
            sender_id, total_messages = packet[1], packet[2]

            # O END tambem e confirmado. Se o ACK se perder, o remetente
            # retransmite END e este peer responde novamente.
            self.end_totals[sender_id] = total_messages
            self._send_packet(
                ('END_ACK', sender_id, self.peer_id),
                address
            )
            self._check_sender_finished(sender_id)

        elif message_type == 'END_ACK':
            target_sender, receiver_id = packet[1], packet[2]
            if target_sender == self.peer_id:
                with self.ack_condition:
                    self.end_acks.add(receiver_id)
                    self.ack_condition.notify_all()

    def run(self):
        print('UDP receiver started. Waiting for messages...')

        linger_deadline = None

        try:
            while True:
                # Quando todos os peers terminaram, mantem o receptor ativo
                # por um periodo para responder ACKs de retransmissoes tardias.
                if len(self.completed_senders) == self.peer_count:
                    if linger_deadline is None:
                        linger_deadline = time.monotonic() + LINGER_TIME

                    remaining = linger_deadline - time.monotonic()
                    if remaining <= 0:
                        break

                    self.sock.settimeout(remaining)
                else:
                    self.sock.settimeout(None)

                try:
                    packet_data, address = self.sock.recvfrom(65535)
                except timeout:
                    if linger_deadline is not None:
                        break
                    continue

                try:
                    packet = pickle.loads(packet_data)
                    self._handle_packet(packet, address)
                except (pickle.PickleError, IndexError, TypeError, ValueError) as error:
                    print('Invalid UDP packet received:', error)

        finally:
            self.sock.settimeout(None)

        print('All peers finished. FIFO log completed.')


def send_reliably(handler, peer_ids, packet_type, sequence=None, total_messages=None):
    """
    Envia READY, DATA ou END e retransmite para quem ainda nao confirmou.

    O ACK identifica o peer que recebeu a mensagem. Assim, cada destinatario
    e confirmado individualmente.
    """
    def packet_for(peer_id):
        if packet_type == 'READY':
            return ('READY', myself)
        if packet_type == 'DATA':
            return ('DATA', myself, sequence)
        return ('END', myself, total_messages)

    def is_acknowledged(peer_id):
        with handler.ack_condition:
            if packet_type == 'READY':
                return peer_id in handler.ready_acks
            if packet_type == 'DATA':
                return (sequence, peer_id) in handler.message_acks
            return peer_id in handler.end_acks

    def send_to(peer_id):
        packet = packet_for(peer_id)
        udpSocket.sendto(
            pickle.dumps(packet),
            (PEERS[peer_id], PEER_UDP_PORT)
        )

    # Primeiro envio.
    pending = set(peer_ids)
    for peer_id in pending:
        send_to(peer_id)

    # Aguarda ACKs e retransmite apenas para os destinatarios pendentes.
    for attempt in range(MAX_RETRIES + 1):
        time.sleep(ACK_TIMEOUT)
        pending = {
            peer_id for peer_id in peer_ids
            if not is_acknowledged(peer_id)
        }

        if not pending:
            return

        if attempt == MAX_RETRIES:
            break

        print(
            f'{packet_type}: retransmitting to peers {sorted(pending)} '
            f'(attempt {attempt + 1})'
        )

        for peer_id in pending:
            send_to(peer_id)

    raise RuntimeError(
        f'No ACK received for {packet_type} from peers: {sorted(pending)}'
    )


# Servidor XML-RPC do peer: recebe comandos de inicializacao.
peerServer = SimpleXMLRPCServer(
    ('0.0.0.0', PEER_TCP_PORT),
    allow_none=True
)
peerServer.register_function(start_peer, 'start_peer')

threading.Thread(
    target=peerServer.serve_forever,
    daemon=True
).start()


# Programa principal.
registerWithGroupManager()

while True:
    print('Waiting for signal to start...')
    myself, nMsgs = start_queue.get()

    print('I am up, and my ID is:', myself)

    if nMsgs == 0:
        print('Terminating.')
        peerServer.shutdown()
        peerServer.server_close()
        udpSocket.close()
        break

    PEERS = getListOfPeers()
    peer_ids = list(range(len(PEERS)))

    if len(peer_ids) != N:
        raise RuntimeError(
            f'Expected {N} peers, but Group Manager returned {len(peer_ids)}.'
        )

    # Inicia o receptor antes de enviar qualquer mensagem.
    msgHandler = MsgHandler(udpSocket, len(PEERS), myself)
    msgHandler.start()

    # Etapa 1: handshake confirmado por ACK.
    print('Sending handshakes...')
    send_reliably(msgHandler, peer_ids, 'READY')

    # Etapa 2: envia todas as mensagens sem aguardar cada ACK individual.
    # Isso permite que a rede entregue pacotes fora de ordem; o receptor
    # utiliza seu buffer para garantir a entrega FIFO.
    print('Sending messages...')
    pending_messages = {
        (sequence, peer_id)
        for sequence in range(nMsgs)
        for peer_id in peer_ids
    }

    for sequence in range(nMsgs):
        packet = pickle.dumps(('DATA', myself, sequence))
        for peer_id in peer_ids:
            udpSocket.sendto(packet, (PEERS[peer_id], PEER_UDP_PORT))
        print(f'Sent message {sequence}')
        

    for attempt in range(MAX_RETRIES + 1):
        time.sleep(ACK_TIMEOUT)

        with msgHandler.ack_condition:
            pending_messages = {
                (sequence, peer_id)
                for sequence, peer_id in pending_messages
                if (sequence, peer_id) not in msgHandler.message_acks
            }

        if not pending_messages:
            break

        if attempt == MAX_RETRIES:
            break

        print(
            f'DATA: retransmitting {len(pending_messages)} '
            f'unconfirmed deliveries (attempt {attempt + 1})'
        )

        for sequence, peer_id in pending_messages:
            packet = pickle.dumps(('DATA', myself, sequence))
            udpSocket.sendto(packet, (PEERS[peer_id], PEER_UDP_PORT))

    if pending_messages:
        raise RuntimeError(
            f'No ACK received for messages: {sorted(pending_messages)}'
        )

    # Etapa 3: informa quantas mensagens foram enviadas e aguarda confirmacao.
    send_reliably(
        msgHandler,
        peer_ids,
        'END',
        total_messages=nMsgs
    )

    # Aguarda o recebimento de END de todos os peers e a conclusao da entrega FIFO.
    msgHandler.join()

    # Salva o log ja ordenado por remetente.
    log_filename = f'logfile{myself}.log'
    with open(log_filename, 'w', encoding='utf-8') as log_file:
        log_file.write(str(msgHandler.logList))

    print('Messages received in FIFO order:')
    print(msgHandler.logList)

    # Envia o log ao Comparison Server por XML-RPC.
    print('Sending the list of messages to the server for comparison...')
    client = ServerProxy(f'http://{SERVER_ADDR}:{SERVER_PORT}')
    client.receive_log(msgHandler.logList)
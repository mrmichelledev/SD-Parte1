from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
from constMP import *
import threading
import queue

# Fila utilizada para armazenar os logs recebidos dos peers.
log_queue = queue.Queue()


def receive_log(logList):
    """Recebe o registro de mensagens de um peer por XML-RPC."""
    print('Received log from peer')
    log_queue.put(logList)
    return True


# Servidor XML-RPC que recebe os logs.
server = SimpleXMLRPCServer(
    ('0.0.0.0', SERVER_PORT),
    allow_none=True
)
server.register_function(receive_log, 'receive_log')

threading.Thread(
    target=server.serve_forever,
    daemon=True
).start()


def promptUser():
    return int(input(
        'Enter the number of messages for each peer to send (0 to terminate)=> '
    ))


def startPeers(peerList, nMsgs):
    """Solicita a inicializacao de cada peer por XML-RPC."""
    for peerNumber, peer in enumerate(peerList):
        client = ServerProxy(f'http://{peer}:{PEER_TCP_PORT}')
        response = client.start_peer(peerNumber, nMsgs)
        print(response)


def check_fifo(logList, expected_messages):
    """
    Verifica FIFO separadamente para cada remetente.

    A ordem entre mensagens de remetentes diferentes pode variar.
    O que deve ser preservado e a sequencia de cada remetente.
    """
    next_expected = {peer_id: 0 for peer_id in range(N)}
    violations = 0
    invalid_messages = 0

    for message in logList:
        if not isinstance(message, (tuple, list)) or len(message) != 2:
            invalid_messages += 1
            continue

        sender_id, sequence = message

        if sender_id not in next_expected:
            invalid_messages += 1
            continue

        if sequence != next_expected[sender_id]:
            violations += 1
        else:
            next_expected[sender_id] += 1

    missing_messages = 0
    for sender_id in range(N):
        missing_messages += max(
            0,
            expected_messages - next_expected[sender_id]
        )

    return violations, missing_messages, invalid_messages


def waitForLogsAndCompare(nMsgs):
    """Aguarda os logs de todos os peers e verifica a propriedade FIFO."""
    logs = []

    for _ in range(N):
        logs.append(log_queue.get())

    print('\nFIFO verification:')

    total_violations = 0
    total_missing = 0
    total_invalid = 0

    for receiver_number, logList in enumerate(logs):
        violations, missing, invalid = check_fifo(logList, nMsgs)

        total_violations += violations
        total_missing += missing
        total_invalid += invalid

        print(
            f'Receiver {receiver_number}: '
            f'{len(logList)} messages, '
            f'FIFO violations: {violations}, '
            f'missing messages: {missing}, '
            f'invalid messages: {invalid}'
        )

    print(
        f'Final result: {total_violations} FIFO violations, '
        f'{total_missing} missing messages, '
        f'{total_invalid} invalid messages.'
    )


def mainLoop():
    while True:
        nMsgs = promptUser()

        groupManager = ServerProxy(
            f'http://{GROUPMNGR_ADDR}:{GROUPMNGR_TCP_PORT}'
        )

        peerList = groupManager.list_peers()
        print('List of Peers:', peerList)

        startPeers(peerList, nMsgs)

        if nMsgs != 0:
            print('Waiting for message logs from the peers...')
            waitForLogsAndCompare(nMsgs)
        else:
            print('Stopping...')
            groupManager.stop_server()
            server.shutdown()
            server.server_close()
            break


mainLoop()
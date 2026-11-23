
from xmlrpc.server import SimpleXMLRPCServer
from threading import Thread
from constMP import *

# Porta utilizada pelo Group Manager para receber chamadas XML-RPC
port = GROUPMNGR_TCP_PORT

# Lista que armazena os peers registrados no sistema
membership = []


# Função chamada remotamente para registrar um novo peer
def register_peer(ipaddr, peer_port):

    # Adiciona o endereço e a porta UDP do peer à lista de participantes
    membership.append((ipaddr, peer_port))

    print('Registered peer:', ipaddr, peer_port)

    return True


# Retorna a lista de endereços dos peers registrados
def list_peers():

    peers = []

    for peer in membership:
        peers.append(peer[0])

    print('List of peers sent to server:', peers)

    return peers


# Encerra o servidor Group Manager
def stop_server():

    print('Stopping Group Manager...')

    # Executa o shutdown em outra thread para evitar bloqueio
    Thread(target=server.shutdown, daemon=True).start()

    return True


# Cria o servidor XML-RPC
# O endereço 0.0.0.0 permite receber conexões de outros processos
server = SimpleXMLRPCServer(
    ('0.0.0.0', port),
    allow_none=True
)

# Registra as funções que poderão ser chamadas remotamente
server.register_function(register_peer, 'register_peer')
server.register_function(list_peers, 'list_peers')
server.register_function(stop_server, 'stop_server')

print(f'Group Manager listening on port {port}...')

# Mantém o servidor ativo aguardando chamadas XML-RPC
server.serve_forever()

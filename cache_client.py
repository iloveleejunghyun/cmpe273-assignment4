import sys
import socket
import time

from sample_data import USERS
from server_config import NODES
from pickle_hash import serialize_GET, serialize_PUT, deserialize
from node_ring import NodeRing
from _rendezvous import RHWNodeRing
from virtual_consist_hash import VirtualCH

BUFFER_SIZE = 1024

# ring = NodeRing(nodes=[0, 1, 2, 3])
# ring = RHWNodeRing(nodes=[0, 1, 2, 3])
ring = VirtualCH([0, 1, 2, 3], 1000)


class UDPClient():
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)

    def send(self, request):
        print('Connecting to server at {}:{}'.format(self.host, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(request, (self.host, self.port))
            s.settimeout(3)
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.timeout:
            raise socket.timeout
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()


def process(udp_clients):
    hash_codes = set()
    # PUT all users.
    for u in USERS:
        data_bytes, key = serialize_PUT(u)

        # hash
        fix_me_server_id = ring.get_node(key)
        print(f"put key={key} to server_id={fix_me_server_id}")
        response = udp_clients[fix_me_server_id].send(data_bytes)

        # TODO set back-up server as ((server_id +1) % n)
        # fix_me_server_id = (fix_me_server_id+1) % len(udp_clients)
        # print(f"put back up key={key} to server_id={fix_me_server_id}")
        # response = udp_clients[fix_me_server_id].send(data_bytes)

        hash_codes.add(response)
        print(response)

    print(
        f"Number of Users={len(USERS)}\nNumber of Users Cached={len(hash_codes)}")

    # TODO: PART I
    # GET all users.
    for hc in hash_codes:
        hc = str(hc, encoding='utf-8')
        # hc = deserialize(hc)
        print(hc)
        data_bytes, key = serialize_GET(hc)
        # fix_me_server_id = 0

        # hash
        fix_me_server_id = ring.get_node(key)
        print(f"get key={key} from server_id={fix_me_server_id}")
        try:
            response = udp_clients[fix_me_server_id].send(data_bytes)
        except socket.timeout:
            # TODO if the server is down, then get back-up data from ((server_id +1) % n)
            fix_me_server_id = (fix_me_server_id+1) % len(udp_clients)
            print(f"get back up key={key} from server_id={fix_me_server_id}")
            response = udp_clients[fix_me_server_id].send(data_bytes)

            # debug code, check response
        resStr = deserialize(response)
        print(f"get parsed data:{resStr}")


if __name__ == "__main__":
    clients = [
        UDPClient(server['host'], server['port'])
        for server in NODES
    ]
    process(clients)

    time.sleep(10)

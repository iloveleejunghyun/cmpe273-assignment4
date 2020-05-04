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


class UDPClient():
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)

    def send(self, request):
        print('Connecting to server at {}:{}'.format(self.host, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(request, (self.host, self.port))
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()


# ring = NodeRing(nodes=[0, 1, 2, 3])
# ring = RHWNodeRing(nodes=[0, 1, 2, 3])
ring = VirtualCH([0, 1, 2, 3], 1000)
# node = ring.get_node('9ad5794ec94345c4873c4e591788743a')


def process(udp_clients):
    hash_codes = set()
    # PUT all users.
    for u in USERS:
        data_bytes, key = serialize_PUT(u)
        # TODO: PART II - Instead of going to server 0, use Naive hashing to split data into multiple servers
        # fix_me_server_id = 0
        # response = udp_clients[fix_me_server_id].send(data_bytes)

        # todo hash
        fix_me_server_id = ring.get_node(key)
        print(f"key={key},server_id={fix_me_server_id}")
        response = udp_clients[fix_me_server_id].send(data_bytes)

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

        # todo hash
        fix_me_server_id = ring.get_node(key)
        print(f"key={key},server_id={fix_me_server_id}")
        response = udp_clients[fix_me_server_id].send(data_bytes)
        # response = udp_clients[fix_me_server_id].send(data_bytes)

        # print(response)

        # debug code, check response
        resStr = deserialize(response)
        print(f"parsed data:{resStr}")


if __name__ == "__main__":
    clients = [
        UDPClient(server['host'], server['port'])
        for server in NODES
    ]
    process(clients)

    time.sleep(10)

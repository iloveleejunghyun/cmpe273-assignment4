import string
import random
from server_config import NODES
import hashlib
__doc__ = """

Rendezvous hashing based ring of nodes.
#?1 murmur3是什么？可以把一个键转化为一个32位4字节的数字
#?2 weighting scheme是什么？
#?3 original white paper 是什么？
Uses murmur3 to convert key to a 32 bit number and uses the weighing scheme
proposed in the original white paper that introduced Rendezvous hashing.

"""

import mmh3
import socket
import struct


def ip2long(ip):
    """Convert an IP string to long."""
    packedIP = socket.inet_aton(ip)
    return struct.unpack("!L", packedIP)[0]


def murmur(key):
    """Return murmur3 hash of the key as 32 bit signed int."""
    return mmh3.hash(key)


def weight(node, key):
    """Return the weight for the key on node.

    Uses the weighing algorithm as prescibed in the original HRW white paper.

    @params:
        node : 32 bit signed int representing IP of the node.
        key : string to be hashed.

    """
    a = 1103515245
    b = 12345
    hash = murmur(key)
    return (a * ((a * node + b) ^ hash) + b) % (2 ^ 31)


class Ring(object):
    """A ring of nodes supporting rendezvous hashing based node selection."""

    def __init__(self, nodes=None):
        nodes = nodes or {}
        self._nodes = set(nodes)

    def add(self, node):
        self._nodes.add(node)

    def nodes(self):
        return self._nodes

    def remove(self, node):
        self._nodes.remove(node)

    def hash(self, key):
        """Return the node to which the given key hashes to."""
        assert len(self._nodes) > 0
        weights = []
        for node in self._nodes:
            n = (node)
            w = weight(n, key)
            weights.append((w, node))

        _, node = max(weights)
        return node


class RHWNodeRing():

    def __init__(self, nodes):
        assert len(nodes) > 0
        self.ring = Ring(nodes)

    def get_node(self, key_hex):
        return self.ring.hash(key_hex)


def _random_string(K):
    """Returns a random string upto length K."""
    L = random.choice(range(K))
    ret = []
    for _ in range(L):
        ret.append(random.choice(string.hexdigits))
    return ''.join(ret)


def test():
    ring = RHWNodeRing(nodes=[0, 1, 2, 3])
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))
    counts = {
        0: 0,
        1: 0,
        2: 0,
        3: 0
    }
    keys = [_random_string(100) for _ in range(100)]
    for k in keys:
        n = ring.get_node(k)
        counts[n] += 1

    print(counts)


# Uncomment to run the above local test via: python3 node_ring.py
test()

from hashlib import md5
from struct import unpack_from
from bisect import bisect_left


class VirtualCH:

    def __init__(self, nodes, virtual_size):
        self.ring = []
        self.hash2node = {}
        self.node_size = len(nodes)
        self.virtual_size = virtual_size
        self.node_stat = [0 for i in range(self.node_size)]
        for n in nodes:
            for v in range(virtual_size):
                h = self._hash(str(n) + str(v))
                self.ring.append(h)
                self.hash2node[h] = n
                # print(n, v, h)
        self.ring.sort()

    def get_node(self, key):
        h = self._hash(str(key))
        n = bisect_left(self.ring, h) % (self.node_size * self.virtual_size)

        self.node_stat[self.hash2node[self.ring[n]]] += 1
        return self.hash2node[self.ring[n]]

    def get_node_stat(self):
        return self.node_stat

    def _hash(self, value):
        k = md5(value.encode('utf-8')).digest()

        ha = unpack_from(">I", k)[0]
        return ha

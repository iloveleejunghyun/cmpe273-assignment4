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
# ?4 struct 用来做什么？ 结构？ 存储数据？
import struct

# ?5 把ip转换为8个字节，但是需要的不是4字节吗？


def ip2long(ip):
    """Convert an IP string to long."""
    packedIP = socket.inet_aton(ip)
    return struct.unpack("!L", packedIP)[0]

# 6 murmur是一种hash算法。把一个字符串或者其他的键转化为32位数字。类比md5，是转化成32字节的字符。


def murmur(key):
    """Return murmur3 hash of the key as 32 bit signed int."""
    return mmh3.hash(key)

# 7 设置每个值的权重? 如何判断权重？
# ip不是键？
# ?8 这玩意儿计算出来的是个啥？


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

# 9 制作轮盘


class Ring(object):
    """A ring of nodes supporting rendezvous hashing based node selection."""

    def __init__(self, nodes=None):
        # ?10 程序怎么知道选哪个？ nodes是一个集合。
        nodes = nodes or {}
        self._nodes = set(nodes)

    def add(self, node):
        self._nodes.add(node)

    def nodes(self):
        return self._nodes

    def remove(self, node):
        self._nodes.remove(node)

    # ？ 11 通过键返回值，对应的节点?是数字还是ip？
    def hash(self, key):
        """Return the node to which the given key hashes to."""
        assert len(self._nodes) > 0
        # ？12weights要怎么用?
        weights = []
        for node in self._nodes:
            # 遍历node，把所有的节点转化成对应的8字节ip
            n = (node)
            # 传入weight函数，计算当前的key在这个node上的权重。
            # ?13当前key是32位数字还是原本的对象键？ 应该是数字。 外部就已经转换好了。
            w = weight(n, key)
            # 所有的weight都计算好后，取出最重的那个节点
            weights.append((w, node))

        # 这里的办法有点巧妙，把权重和节点当做元组传入数组，然后根据权重来对数组进行排序，找出其中最大的一组元组，然后取出其中的节点。
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
    # ？13 这个是什么函数？ 选择？ 从K范围中选择一个什么？
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

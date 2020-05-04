from hashlib import md5
from struct import unpack_from
from bisect import bisect_left

# # ?1有1000万个数
# ITEMS = 100000
# # 节点100个， 虚拟节点1000个。虚拟节点一般都比物理节点多。
# NODES = 4
# VNODES = 1000

# # 节点状态。用来测试有的节点抛锚了怎么办。
# node_stat = [0 for i in range(NODES)]

# # ?2 这里使用hash的用途是？


# def _hash(value):

#     k = md5(value.encode('utf-8')).digest()
#     # k = md5(str(value).encode('utf-8')).digest()
#     # k = md5(bytes(value, 'utf-8')).digest()

#     ha = unpack_from(">I", k)[0]
#     return ha


# ring = []
# hash2node = {}

# # 在所有物理节点上遍历虚拟节点
# # ？6 我还以为每个物理节点对应几个虚拟几点，分组。 但是这里好像是每个物理节点对应所有的虚拟节点？
# for n in range(NODES):
#     for v in range(VNODES):
#         # 3把当前的物理节点和虚拟节点的字符串合并，然后计算一个对应的hash值。但是这个hash值为什么只取一个字符？ 这里不是一个字符。
#         h = _hash(str(n) + str(v))
#         # ？4 添加到轮盘里面去，ring是个数组。有什么用？
#         ring.append(h)
#         # hash值到node物理节点有一个映射，h->n,通过hash值可以找到节点n
#         hash2node[h] = n
#         print(n, v, h)

# # ? 5这里为什么要排序？
# print(f"sorting...， ring size={len(ring)}")
# ring.sort()

# print("assinging...")
# # 遍历所有键，obj
# for item in range(ITEMS):
#     # 对每个项计算其hash值。 之前每个物理节点与其虚拟节点都计算了hash值。
#     h = _hash(str(item))
#     #  7 后面这个操作是什么？ 这里返回了h这个hash值在ring轮盘中的索引index。这里解释了为什么前面需要对轮盘排序。因为这里取索引值需要。也可以用hash，就是更大隔，
#     #  8 但是后面为什么要对物理节点数量乘以虚拟节点数量的积取余数？这个索引应该超不过其积吧？ 为什么超了？的确是超了，我傻了。物理节点100，虚拟节点1000.总计10万个节点位置。但是这里有1000万数据呀，肯定超。
#     # ？ 9 这里感觉有点复杂。项的hash值作为轮盘上某个hash值，找其在轮盘上的索引，然后根据索引？ 不，是hash。窝草。这写法容易晕人啊。ring[n]不是倒回去了？ 不就是h? 也有可能不是。不懂得为什么。
#     n = bisect_left(ring, h) % (NODES*VNODES)
#     # 然后通过索引找到的hash值去找对应的物理node，将其状态+1.这里表示上面拥有的obj多了1个。
#     node_stat[hash2node[ring[n]]] += 1
#     # print(item, NODES*VNODES)

# print("calculate...")
# # 打印最后的每个物理虚拟节点状态，就是obj在上面的数量。
# print(sum(node_stat), node_stat, len(node_stat))

# _ave = ITEMS / NODES
# # 最大拥有的节点数和最小拥有的节点数。)
# _max = max(node_stat)
# _min = min(node_stat)

# # 这是在算比例？ 最多溢出比例，最少不足比例？ 取余什么意思？ 没看懂。
# print("Ave: %d" % _ave)
# print("Max: %d\t(%0.2f%%)" % (_max, (_max - _ave) * 100.0 / _ave))
# print("Min: %d\t(%0.2f%%)" % (_min, (_ave - _min) * 100.0 / _ave))


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
                print(n, v, h)
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
        # k = md5(str(value).encode('utf-8')).digest()
        # k = md5(bytes(value, 'utf-8')).digest()

        ha = unpack_from(">I", k)[0]
        return ha


vch = VirtualCH([0, 1, 2, 3], 1000)
for item in range(100000):
    n = vch.get_node(item)
    # print(n)
print(vch.get_node_stat())

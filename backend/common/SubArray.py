# 为了避免 python 中各种深浅拷贝的问题, 用一个子数组类
class SubArray(object):
    def __init__(self, raw: bytearray | bytes, start: int, end: int):
        self.raw = raw
        self.start = start
        self.end = end

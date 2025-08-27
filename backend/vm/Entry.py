'''
对于一条记录来说,使用 Entry 类维护了其结构
对于字段的更新操作由后面的表和字段管理 TBM 实现
所以在 VM 的实现中,一条记录只有一个版本
一条记录存储在一条 Data Item 中,所以 Entry 中保存一个 DataItem 的引用即可
'''
# VM向上层抽象出entry
# entry结构：
# [XMIN] [XMAX] [data]
# XMIN 是创建该条记录(版本)的事务编号,而 XMAX 则是删除该条记录(版本)的事务编号
# DATA 就是这条记录持有的数据

import struct
from backend.common.SubArray import SubArray
from backend.dm.dataItem import DataItem

OF_XMIN = 0
OF_XMAX = OF_XMIN + 8
OF_DATA = OF_XMAX + 8
def newEntry(vm, dataItem, uid):
    return Entry(vm, dataItem, uid)

def loadEntry(vm, uid):
    di = vm.dm.read(uid)
    return Entry(vm, di, uid)

def wrapEntryRaw(xid, data):
    xmin = struct.pack(">q", xid)
    xmax = bytearray(8)
    return xmin + xmax + data

class Entry(object):
    def __init__(self, vm, dataItem, uid):
        if dataItem == None:
            return None
        self.vm = vm
        self.dataItem = dataItem
        self.uid = uid
    
    def release(self):
        self.vm.releaseEntry(self)

    def remove(self):
        self.dataItem.release()

    def data(self):
        sa = self.dataItem.data()
        data = sa.raw[sa.start + OF_DATA : sa.end]
        return data

    def getXmin(self):
        self.dataItem.rLock.acquire()
        try:
            sa = self.dataItem.data()
            buf = sa.raw[sa.start + OF_XMIN : sa.start + OF_XMAX]
            return struct.unpack('>q', buf)[0]
        finally:
            self.dataItem.rLock.release()
    
    def getXmax(self):
        self.dataItem.rLock.acquire()
        try:
            sa = self.dataItem.data()
            buf = sa.raw[sa.start + OF_XMAX : sa.start + OF_DATA]
            return struct.unpack('>q', buf)[0]
        finally:
            self.dataItem.rLock.release()

    def setXmax(self, xid):
        self.dataItem.before()
        sa = self.dataItem.data()
        sa.raw[sa.start + OF_XMAX : sa.start + OF_XMAX + 8] = struct.pack('>q', xid)
        self.dataItem.after(xid)

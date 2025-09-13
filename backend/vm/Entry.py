# VM 向上层抽象出 entry
# entry 结构:
# [XMIN] [XMAX] [data]
# XMIN 是创建该条记录(版本)的事务编号, 而 XMAX 则是删除该条记录(版本)的事务编号
# DATA 就是这条记录持有的数据

import struct
from backend.dm.dataItem.DataItem import DataItem

OF_XMIN = 0
OF_XMAX = OF_XMIN + 8
OF_DATA = OF_XMAX + 8

class Entry(object):
    def __init__(self, vm, dataItem: DataItem, uid: int):
        if dataItem == None:
            return None
        self.vm = vm
        self.dataItem = dataItem
        self.uid = uid
    
    def release(self) -> None:
        self.vm.releaseEntry(self)

    def remove(self) -> None:
        self.dataItem.release()

    def data(self) -> bytearray | bytes:
        '''
        获取记录中持有的数据
        '''
        sa = self.dataItem.data()
        data = sa.raw[sa.start + OF_DATA : sa.end]
        return data

    def getXmin(self) -> int:
        self.dataItem.rLock.acquire()
        try:
            sa = self.dataItem.data()
            buf = sa.raw[sa.start + OF_XMIN : sa.start + OF_XMAX]
            return struct.unpack('>q', buf)[0]
        finally:
            self.dataItem.rLock.release()
    
    def getXmax(self) -> int:
        self.dataItem.rLock.acquire()
        try:
            sa = self.dataItem.data()
            buf = sa.raw[sa.start + OF_XMAX : sa.start + OF_DATA]
            return struct.unpack('>q', buf)[0]
        finally:
            self.dataItem.rLock.release()

    def setXmax(self, xid: int) -> None:
        self.dataItem.before()
        sa = self.dataItem.data()
        sa.raw[sa.start + OF_XMAX : sa.start + OF_XMAX + 8] = struct.pack('>q', xid)
        self.dataItem.after(xid)

def newEntry(vm, dataItem: DataItem, uid: int) -> Entry:
    '''
    构造一个新的 Entry
    '''
    return Entry(vm, dataItem, uid)

def loadEntry(vm, uid: int) -> Entry:
    '''
    从 dm 读取 DataItem 构造为一个 Entry 
    '''
    di = vm.dm.read(uid)
    return Entry(vm, di, uid)

def wrapEntryRaw(xid: int, data: bytearray | bytes) -> bytearray | bytes:
    '''
    创建记录时调用, 记录创建该条记录的事务编号
    '''
    xmin = struct.pack(">q", xid)
    xmax = bytearray(8)
    return xmin + xmax + data

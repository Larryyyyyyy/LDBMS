# DataItem 是 DM 层向上层提供的数据抽象
# 上层模块通过地址向 DM 请求到对应的 DataItem ,再获取到其中的数据
# DataItem 中保存的数据，结构如下：
# [ValidFlag] [DataSize] [Data]
import struct
import threading
from backend.common.SubArray import SubArray

OF_VALID = 0
OF_SIZE = 1
OF_DATA = 3
def wrapDataItemRaw(raw):
    valid_byte = bytearray(1)
    size_bytes = struct.pack('>h', len(raw))
    return valid_byte + size_bytes + raw

# 从页面的 offset 处解析处 dataitem
def parseDataItem(pg, offset, dm):
    raw = pg.data
    size = struct.unpack('>h', raw[offset + OF_SIZE : offset + OF_DATA])[0]
    length = (size + OF_DATA) & ((1 << 16) - 1)
    uid = (pg.pageNumber << 32) | offset
    return DataItem(SubArray(raw, offset, offset + length), bytearray(length), pg, uid, dm)
    
def setDataItemRawInvalid(raw):
    raw[OF_VALID : OF_VALID + 1] = struct.pack('B', 1)

class DataItem(object):
    def __init__(self, raw, oldRaw, pg, uid, dm):
        self.raw = raw
        self.oldRaw = oldRaw
        self.dm = dm
        self.uid = uid
        self.pg = pg
        self.rLock = threading.RLock()
        self.wLock = threading.RLock()
    
    def isValid(self):
        return self.raw.raw[self.raw.start + OF_VALID] == 0
    
    def data(self):
        return SubArray(self.raw.raw, self.raw.start + OF_DATA, self.raw.end)
    
    def before(self):
        self.wLock.acquire()
        self.pg.setDirty(True)
        self.oldRaw[0 : len(self.oldRaw)] = self.raw.raw[self.raw.start : self.raw.start + len(self.oldRaw)]

    def unBefore(self):
        for i in range(self.raw.start, self.raw.start + len(self.oldRaw)):
            self.raw.raw[i] = self.oldRaw[i - self.raw.start]
        self.wLock.release()
    
    def after(self, xid):
        self.dm.logDataItem(xid, self)
        self.wLock.release()

    def release(self):
        self.dm.releaseDataItem(self)

# DataItem 是 DM 层向上层提供的数据抽象
# 上层模块通过地址向 DM 请求到对应的 DataItem, 再获取到其中的数据
# DataItem 中保存的数据, 结构如下：
# [ValidFlag] [DataSize] [Data]
import struct
import threading
from backend.common.SubArray import SubArray
from backend.dm.page.Page import Page

# 段位置标记
OF_VALID = 0
OF_SIZE = 1
OF_DATA = 3

class DataItem(object):
    def __init__(self, raw: SubArray, oldRaw: bytearray, pg: Page, uid: int, dm):
        self.raw = raw
        self.oldRaw = oldRaw
        self.dm = dm
        self.uid = uid
        self.pg = pg
        self.rLock = threading.RLock()
        self.wLock = threading.RLock()
    
    def isValid(self) -> bool:
        '''
        根据首位判断是否合法, 被删除的 DataItem 会被设置为非法
        '''
        return self.raw.raw[self.raw.start + OF_VALID] == 0
    
    def data(self) -> SubArray:
        '''
        把数据段打包成 SubArray
        '''
        return SubArray(self.raw.raw, self.raw.start + OF_DATA, self.raw.end)
    
    def before(self) -> None:
        '''
        准备对 DataItem 修改
        把 raw 数据备份到 oldRaw
        '''
        self.wLock.acquire()
        self.pg.setDirty(True)
        self.oldRaw[0 : len(self.oldRaw)] = self.raw.raw[self.raw.start : self.raw.start + len(self.oldRaw)]

    def unBefore(self) -> None:
        '''
        撤回对 DataItem 修改
        把 oldRaw 数据回溯到 raw
        '''
        for i in range(self.raw.start, self.raw.start + len(self.oldRaw)):
            self.raw.raw[i] = self.oldRaw[i - self.raw.start]
        self.wLock.release()
    
    def after(self, xid: int) -> None:
        '''
        修改完成
        '''
        self.dm.logDataItem(xid, self)
        self.wLock.release()

    def release(self) -> None:
        '''
        释放 DataItem 的缓存
        避免缓存过大
        '''
        self.dm.releaseDataItem(self)

def wrapDataItemRaw(raw: bytearray) -> bytearray:
    '''
    根据 DataItem 规定结构包装一个数据对象
    '''
    valid_byte = bytearray(1)
    size_bytes = struct.pack('>h', len(raw))
    return valid_byte + size_bytes + raw

def parseDataItem(pg: Page, offset: int, dm) -> DataItem:
    '''
    从页面 pg 的 offset 处解析 DataItem
    '''
    raw = pg.data
    size = struct.unpack('>h', raw[offset + OF_SIZE : offset + OF_DATA])[0]
    length = (size + OF_DATA) & ((1 << 16) - 1)
    uid = (pg.pageNumber << 32) | offset
    return DataItem(SubArray(raw, offset, offset + length), bytearray(length), pg, uid, dm)
    
def setDataItemRawInvalid(raw: bytearray) -> None:
    '''
    设置 ValidFlag 为 Invalid
    '''
    raw[OF_VALID : OF_VALID + 1] = struct.pack('B', 1)

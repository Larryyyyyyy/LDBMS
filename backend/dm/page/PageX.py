# 一个普通页面以一个 2 字节无符号数起始,表示这一页的空闲位置的偏移,剩下的部分都是实际存储的数据
import struct
from backend.dm.pageCache import PageCache

OF_FREE = 0
OF_DATA = 2
MAX_FREE_SPACE = (PageCache.PAGE_SIZE) - OF_DATA
    
def initRaw():
    raw = bytearray(PageCache.PAGE_SIZE)
    setFSO(raw, OF_DATA)
    return raw

def setFSO(raw, ofData):
    of_data_bytes = struct.pack('>h', ofData)
    raw[OF_FREE : OF_FREE + OF_DATA] = of_data_bytes

def getFSO_page(pg):                           # 获取 pg 的FSO
    return getFSO_raw(pg.getData())

def getFSO_raw(raw):
    return struct.unpack('>h', raw[0 : 2])[0]

def insert(pg, raw):                           # 将 raw 插入 pg 中,返回插入位置
    pg.setDirty(True)
    offset = getFSO_raw(pg.getData())
    pg.data[offset : offset + len(raw)] = raw
    setFSO(pg.data, (offset + len(raw)) & ((1 << 16) - 1))
    return offset

def getFreeSpace(pg):                          # 获取页面的空闲空间大小
    return (PageCache.PAGE_SIZE) - (getFSO_raw(pg.data) & ((1 << 32) - 1))

def recoverInsert(pg, raw, offset):            # 将 raw 插入 pg 中的 offset 位置,并将 pg 的 offset 设置为较大的 offset
    pg.setDirty(True)
    pg.getData()[offset : offset + len(raw)] = raw
    rawFSO = getFSO_raw(pg.getData())
    if rawFSO < offset + len(raw):
        setFSO(pg.getData(), (offset + len(raw)) & ((1 << 16) - 1))

def recoverUpdate(pg, raw, offset):            # 将 raw 插入 pg 中的 offset 位置,不更新 update
    pg.setDirty(True)
    pg.getData()[offset : offset + len(raw)] = raw
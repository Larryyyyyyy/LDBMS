# 第一页: 特殊管理页
# 在每次数据库启动时,会生成一串随机字节,存储在 100 ~ 107 字节.在数据库正常关闭时,会将这串字节拷贝到第一页的 108 ~ 115 字节.
import os
from backend.dm.pageCache import PageCache
from backend.dm.page.Page import Page

OF_VC = 100
LEN_VC = 8

def InitRaw() -> bytearray | bytes:
    raw = bytearray(PageCache.PAGE_SIZE)
    setVcOpen_raw(raw)
    return raw

def setVcOpen_raw(raw: bytearray | bytes) -> None:
    '''
    打开时传递随机字节
    '''
    random_vc_bytes = os.urandom(LEN_VC)
    raw[OF_VC : OF_VC + LEN_VC] = random_vc_bytes

def setVcOpen_page(pg: Page) -> None:
    pg.setDirty(True)
    setVcOpen_raw(pg.data)

def setVcClose_raw(raw: bytearray | bytes):
    '''
    关闭时复制随机字节
    '''
    raw[OF_VC + LEN_VC : OF_VC + 2 * LEN_VC] = raw[OF_VC : OF_VC + LEN_VC]

def setVcClose_page(pg: Page) -> None:
    pg.setDirty(True)
    setVcClose_raw(pg.data)

def checkVc_raw(raw: bytearray | bytes) -> bool:
    '''
    数据库在每次启动时, 都会检查第一页两处的字节是否相同, 以此来判断上一次是否正常关闭
    如果是异常关闭, 就需要执行数据的恢复流程
    '''
    return raw[OF_VC : OF_VC + LEN_VC] == raw[OF_VC + LEN_VC : OF_VC + 2 * LEN_VC]

def checkVc_page(pg: Page) -> bool:
    return checkVc_raw(pg.getData())

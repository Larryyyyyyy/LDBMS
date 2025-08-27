# 第一页:特殊管理页
# 在每次数据库启动时,会生成一串随机字节,存储在 100 ~ 107 字节.在数据库正常关闭时,会将这串字节拷贝到第一页的 108 ~ 115 字节.
import os
from backend.dm.pageCache import PageCache

OF_VC = 100
LEN_VC = 8

def InitRaw():
    raw = bytearray(PageCache.PAGE_SIZE)
    setVcOpen_raw(raw)
    return raw

def setVcOpen_page(pg):
    pg.setDirty(True)
    setVcOpen_raw(pg.getData())

def setVcOpen_raw(raw):                        # 打开时传递随机字节
    random_vc_bytes = os.urandom(LEN_VC)
    raw[OF_VC : OF_VC + LEN_VC] = random_vc_bytes

def setVcClose_page(pg):
    pg.setDirty(True)
    setVcClose_raw(pg.getData())

def setVcClose_raw(raw):                       # 关闭时复制随机字节
    raw[OF_VC + LEN_VC : OF_VC + 2 * LEN_VC] = raw[OF_VC : OF_VC + LEN_VC]

def checkVc_page(pg):
    return checkVc_raw(pg.getData())

def checkVc_raw(raw):
    return raw[OF_VC : OF_VC + LEN_VC] == raw[OF_VC + LEN_VC : OF_VC + 2 * LEN_VC]
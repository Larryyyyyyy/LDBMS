# 单线程: 假设日志中最后一个事务是 Ti
# 对 Ti 之前的事务的日志进行重做, 然后若 Ti 是已完成(committed, aborted), 将 Ti 重做, 否则进行撤销

# 多线程: 
# 正在进行的事务, 不会读取其他任何未提交的事务产生的数据
# 正在进行的事务, 不会修改其他任何未提交的事务修改或产生的数据
import struct
from backend.dm.dataItem import DataItem
from backend.dm.page import PageX
from backend.tm.TransactionManager import TransactionManager
from backend.dm.logger.Logger import Logger
from backend.dm.pageCache.PageCache import PageCache
from backend.dm.page.Page import Page

# 两种日志的格式
# [LogType] [XID] [Pgno] [Offset] [Raw]
LOG_TYPE_INSERT = 0
# [LogType] [XID] [UID] [OldRaw] [NewRaw]
LOG_TYPE_UPDATE = 1
        
REDO = 0
UNDO = 1

# 插入日志参数位置
OF_TYPE = 0
OF_XID = OF_TYPE + 1
OF_UPDATE_UID = OF_XID + 8
OF_UPDATE_RAW = OF_UPDATE_UID + 8
        
# 更新日志参数位置
OF_INSERT_PGNO = OF_XID + 8
OF_INSERT_OFFSET = OF_INSERT_PGNO + 4
OF_INSERT_RAW = OF_INSERT_OFFSET + 2

# 插入日志
class InsertLogInfo(object):
    def __init__(self, xid: int, pgno: int, offset: int, raw: bytearray | bytes):
        self.xid = xid
        self.pgno = pgno
        self.offset = offset
        self.raw = raw

# 更新日志
class UpdateLogInfo(object):
    def __init__(self, xid: int, pgno: int, offset: int, oldRaw: bytearray | bytes, newRaw: bytearray | bytes):
        self.xid = xid
        self.pgno = pgno
        self.offset = offset
        self.oldRaw = oldRaw
        self.newRaw = newRaw

def recover(tm: TransactionManager, lg: Logger, pc: PageCache) -> None:
    '''
    恢复数据
    根据 Logger 的记录确定最大页数, 并截断
    重做所有已完成的事务
    撤销所有未完成的事务
    '''
    print("Recovering...")

    lg.rewind()
    maxPgno = 0
    while 1:
        log = lg.next()
        if log == None:
            break
        if isInsertLog(log):
            pgno = struct.unpack('>i', log[OF_INSERT_PGNO : OF_INSERT_OFFSET])[0]
        else:
            uid = struct.unpack('>q', log[OF_UPDATE_UID : OF_UPDATE_RAW])[0]
            uid >>= 32
            pgno = uid & ((1 << 32) - 1)
        if pgno > maxPgno:
            maxPgno = pgno
    if maxPgno == 0:
        maxPgno = 1

    pc.truncateByBgno(maxPgno)
    print("Truncate to " + str(maxPgno) + " pages.")
    redoTransactions(tm, lg, pc)
    print("Redo Transactions Over.")
    undoTransactions(tm, lg, pc)
    print("Undo Transactions Over.")
    print("Recovery Over.")

def redoTransactions(tm: TransactionManager, lg: Logger, pc: PageCache) -> None:
    '''
    挨个访问记录
    重做所有已完成的事务
    '''
    lg.rewind()
    while True:
        log = lg.next()
        if log == None:
            break
        if isInsertLog(log):
            xid = struct.unpack('>q', log[OF_XID : OF_INSERT_PGNO])[0]   
            if tm.isActive(xid) == False:
                doInsertLog(pc, log, REDO)
        else:
            xid = struct.unpack('>q', log[OF_XID : OF_UPDATE_UID])[0]
            if tm.isActive(xid) == False:
                doUpdateLog(pc, log, REDO)

def undoTransactions(tm: TransactionManager, lg: Logger, pc: PageCache) -> None:
    '''
    挨个访问记录
    撤销所有未完成的事务
    '''
    logCache = {}
    lg.rewind()
    while 1:
        log = lg.next()
        if log == None:
            break
        if isInsertLog(log):
            xid = struct.unpack('>q', log[OF_XID : OF_INSERT_PGNO])[0]
            if tm.isActive(xid):
                if logCache.get(xid) == None:
                    logCache[xid] = []
                logCache.get(xid).append(log)
        else:
            xid = struct.unpack('>q', log[OF_XID : OF_UPDATE_UID])[0]
            if tm.isActive(xid):
                if logCache.get(xid) == None:
                    logCache[xid] = []
                logCache.get(xid).append(log)
    for key in logCache:
        logs = logCache[key]
        for i in range(len(logs) - 1, -1, -1):
            log = logs[i]
            if isInsertLog(log):
                doInsertLog(pc, log, UNDO)
            else:
                doUpdateLog(pc, log, UNDO)
        tm.abort(key)

def isInsertLog(log: bytearray | bytes) -> bool:
    '''
    判断该事务是否是 InsertLog
    '''
    return log[0] == LOG_TYPE_INSERT

def insertLog(xid: int, pg: Page, raw: bytearray | bytes) -> bytes | bytearray:
    '''
    插入日志打包
    [插入日志标记][xid][页数][偏移量][本身的内容]
    '''
    logTypeRaw = struct.pack("B", LOG_TYPE_INSERT)
    xidRaw = struct.pack(">q", xid)
    pgnoRaw = struct.pack(">i", pg.getPageNumber())
    offsetRaw = struct.pack(">h", PageX.getFSO_page(pg))
    return logTypeRaw + xidRaw + pgnoRaw + offsetRaw + raw

def parseInsertLog(log: bytearray | bytes) -> InsertLogInfo:
    '''
    读取更新日志, 写成 InsertLogInfo 类
    '''
    xid = struct.unpack('>q', log[OF_XID : OF_INSERT_PGNO])[0]
    pgno = struct.unpack('>i', log[OF_INSERT_PGNO : OF_INSERT_OFFSET])[0]
    offset = struct.unpack('>h', log[OF_INSERT_OFFSET : OF_INSERT_RAW])[0]
    raw = log[OF_INSERT_RAW : len(log)]
    li = InsertLogInfo(xid, pgno, offset, raw)
    return li

def doInsertLog(pc: PageCache, log: bytearray | bytes, flag: int) -> None:
    '''
    在主页面完成更新
    '''
    li = parseInsertLog(log)
    pg = pc.getPage(li.pgno)
    try:
        if flag == UNDO:
            DataItem.setDataItemRawInvalid(li.raw)
        PageX.recoverInsert(pg, li.raw, li.offset)
    finally:
        pg.release()

def updateLog(xid: int, di: DataItem.DataItem) -> bytearray | bytes:
    '''
    更新日志打包
    [更新日志标记][xid][页数][偏移量][本身的内容]
    '''
    logType = struct.pack(">b", LOG_TYPE_UPDATE)
    xidRaw = struct.pack(">q", xid)
    uidRaw = struct.pack(">q", di.uid)
    oldRaw = di.oldRaw
    raw = di.raw
    newRaw = raw.raw[raw.start : raw.end]
    return logType + xidRaw + uidRaw + oldRaw + newRaw
    
def parseUpdateLog(log: bytearray | bytes) -> UpdateLogInfo:
    '''
    读取更新日志, 写成 UpdateLogInfo 类
    '''
    xid = struct.unpack('>q', log[OF_XID : OF_UPDATE_UID])[0]
    uid = struct.unpack('>q', log[OF_UPDATE_UID : OF_UPDATE_RAW])[0]
    offset = uid & ((1 << 16) - 1)
    uid >>= 32
    pgno = uid & ((1 << 32) - 1)
    length = (len(log) - OF_UPDATE_RAW) // 2
    oldRaw = log[OF_UPDATE_RAW : OF_UPDATE_RAW + length]
    newRaw = log[OF_UPDATE_RAW + length : OF_UPDATE_RAW + length * 2]
    li = UpdateLogInfo(xid, pgno, offset, oldRaw, newRaw)
    return li

def doUpdateLog(pc: PageCache, log: Logger, flag: int) -> None:
    '''
    在主页面完成更新
    '''
    xi = parseUpdateLog(log)
    pgno = xi.pgno
    offset = xi.offset
    try:
        if flag == REDO:
            raw = xi.newRaw
        else:
            raw = xi.oldRaw
        pg = pc.getPage(pgno)
        PageX.recoverUpdate(pg, raw, offset)
    finally:
        pg.release()

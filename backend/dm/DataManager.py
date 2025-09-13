from backend.dm.dataItem import DataItem
from backend.dm.logger import Logger
from backend.dm.page import PageOne
from backend.dm.page import PageX
from backend.dm.pageCache import PageCache
from backend.dm.pageIndex import PageIndex
from backend.common.AbstractCache import AbstractClass
from backend.dm import Recover
from backend.tm.TransactionManager import TransactionManager

class DataManager(AbstractClass):
    def __init__(self, pc: PageCache.PageCache, logger: Logger.Logger, tm: TransactionManager):
        super().__init__(0)
        self.pc = pc
        self.logger = logger
        self.tm = tm
        self.pIndex = PageIndex.PageIndex()
        self.pageOne = None
        
    def read(self, uid: int) -> DataItem.DataItem | None:
        '''
        通过 uid 地址获取对应的 DataItem
        '''
        di = super().get(uid)
        if not di.isValid():
            di.release()
            return None
        return di

    def insert(self, xid: int, data: bytearray) -> int:
        '''
        在 pageIndex 中获取一个足以存储 data 的页号
        把 data 封装写入 logger
        再在主页面留下记录
        '''
        raw = DataItem.wrapDataItemRaw(data)
        if len(raw) > PageX.MAX_FREE_SPACE:
            raise Exception("DataTooLargeException")
        pi = None
        for i in range(5):
            pi = self.pIndex.select(len(raw))
            if pi != None:
                break
            else:
                newPgno = self.pc.newPage(PageX.initRaw())
                self.pIndex.add(newPgno, PageX.MAX_FREE_SPACE)
        if pi == None:
            raise Exception("DatabaseBusyException")
        pg = None
        try:
            pg = self.pc.getPage(pi.pgno)
            log = Recover.insertLog(xid, pg, raw)
            self.logger.log(log)
            offset = PageX.insert(pg, raw)
            pg.release()
            return pi.pgno << 32 | offset
        finally:
            if pg != None:
                self.pIndex.add(pi.pgno, PageX.getFreeSpace(pg))
            else:
                self.pIndex.add(pi.pgno, 0)
    
    def close(self) -> None:
        super(DataManager, self).close()
        PageOne.setVcClose_page(self.pageOne)
        self.pageOne.release()
        self.pc.close()

    def logDataItem(self, xid: int, di: DataItem.DataItem) -> None:
        '''
        把事务及 DataItem 打包成更新日志
        '''
        log = Recover.updateLog(xid, di)
        self.logger.log(log)
        
    def releaseDataItem(self, di: DataItem.DataItem) -> None:
        '''
        在使用完 DataItem 后, 调用 release() 方法释放掉 DataItem 的缓存
        '''
        super(DataManager, self).release(di.uid)

    def getForCache(self, uid: int) -> DataItem.DataItem:
        '''
        从 uid 中解析出页号, 从 PageCache 中获取到页面, 再根据偏移, 解析出 DataItem 
        '''
        offset = (uid & ((1 << 16) - 1))
        uid = (uid >> 32)
        pgno = (uid & ((1 << 32) - 1))
        pg = self.pc.getPage(pgno)
        return DataItem.parseDataItem(pg, offset, self)
    
    def releaseForCache(self, di: DataItem.DataItem) -> None:
        '''
        将 DataItem 写回数据源, 把其所在的页 release 即可
        '''
        di.pg.release()
    
    def initPageOne(self) -> None:
        '''
        在创建文件时初始化 PageOne
        '''
        pgno = self.pc.newPage(PageOne.InitRaw())
        assert pgno == 1
        self.pageOne = self.pc.getPage(pgno)
        self.pc.flushPage(self.pageOne)

    def loadCheckPageOne(self) -> bool:
        '''
        在打开已有文件时读入 PageOne 并验证正确性
        '''
        self.pageOne = self.pc.getPage(1)
        return PageOne.checkVc_page(self.pageOne)

    def fillPageIndex(self) -> None:
        '''
        初始化 pageIndex
        '''
        pageNumber = self.pc.pageNumber
        for i in range(2, pageNumber + 1):
            pg = self.pc.getPage(i)
            self.pIndex.add(pg.pageNumber, PageX.getFreeSpace(pg))
            pg.release()

def create(path: str, mem: int, tm: TransactionManager) -> DataManager:
    '''
    以 path 为目录创建一个数据文件和记录文件
    '''
    # data.db
    pc = PageCache.create(path, mem)
    # data.log
    lg = Logger.create(path)
    dm = DataManager(pc, lg, tm)
    dm.initPageOne()
    return dm

def fileopen(path, mem, tm) -> DataManager:
    '''
    打开 path 数据文件和记录文件
    '''
    # data.db
    pc = PageCache.fileopen(path, mem)
    # data.log
    lg = Logger.fileopen(path)
    dm = DataManager(pc, lg, tm)
    if dm.loadCheckPageOne() == False:
        Recover.recover(tm, lg, pc)
    dm.fillPageIndex()
    PageOne.setVcOpen_page(dm.pageOne)
    dm.pc.flushPage(dm.pageOne)
    return dm

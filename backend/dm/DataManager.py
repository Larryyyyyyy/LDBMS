from backend.dm.dataItem import DataItem
from backend.dm.logger import Logger
from backend.dm.page import PageOne
from backend.dm.page import PageX
from backend.dm.pageCache import PageCache
from backend.dm.pageIndex import PageIndex
from backend.common.AbstractCache import AbstractClass
from backend.dm import Recover

def create(path, mem, tm):
    pc = PageCache.create(path, mem)    # data.db
    lg = Logger.create(path)            # data.log
    dm = DataManager(pc, lg, tm)
    dm.initPageOne()
    return dm

def fileopen(path, mem, tm):
    pc = PageCache.fileopen(path, mem)  # data.db
    lg = Logger.fileopen(path)          # data.log
    dm = DataManager(pc, lg, tm)
    if dm.loadCheckPageOne() == False:
        Recover.recover(tm, lg, pc)
    dm.fillPageIndex()
    PageOne.setVcOpen_page(dm.pageOne)
    dm.pc.flushPage(dm.pageOne)
    return dm

class DataManager(AbstractClass):
    def __init__(self, pc, logger, tm):
        super().__init__(0)
        self.pc = pc
        self.logger = logger
        self.tm = tm
        self.pIndex = PageIndex.PageIndex()
        self.pageOne = None
        
    def read(self, uid):
        di = super().get(uid)
        if not di.isValid():
            di.release()
            return None
        return di

    def insert(self, xid, data):
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
        freeSpace = 0
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
                self.pIndex.add(pi.pgno, freeSpace)
    
    def close(self):
        super(DataManager, self).close()
        PageOne.setVcClose_page(self.pageOne)
        self.pageOne.release()
        self.pc.close()

    def logDataItem(self, xid, di):
        log = Recover.updateLog(xid, di)
        self.logger.log(log)
        
    def releaseDataItem(self, di):
        super(DataManager, self).release(di.uid)

    def getForCache(self, uid):
        offset = (uid & ((1 << 16) - 1))
        uid = (uid >> 32)
        pgno = (uid & ((1 << 32) - 1))
        pg = self.pc.getPage(pgno)
        return DataItem.parseDataItem(pg, offset, self)
    
    def releaseForCache(self, di):
        di.pg.release()
    
    def initPageOne(self):
        pgno = self.pc.newPage(PageOne.InitRaw())
        assert pgno == 1
        self.pageOne = self.pc.getPage(pgno)
        self.pc.flushPage(self.pageOne)

    def loadCheckPageOne(self):
        self.pageOne = self.pc.getPage(1)
        return PageOne.checkVc_page(self.pageOne)

    def fillPageIndex(self):
        pageNumber = self.pc.getPageNumber()
        for i in range(2, pageNumber + 1):
            pg = self.pc.getPage(i)
            self.pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg))
            pg.release()

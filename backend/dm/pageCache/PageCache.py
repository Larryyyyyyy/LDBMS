PAGE_SIZE = 1 << 13
MEM_MIN_LIM = 10
DB_SUFFIX = '.db'

import threading
from backend.dm.page import Page
from backend.common.AbstractCache import AbstractClass

def create(path, memory):
    file = path + DB_SUFFIX
    return PageCache(file, memory // PAGE_SIZE)

def fileopen(path, memory):
    file = path + DB_SUFFIX
    return PageCache(file, memory // PAGE_SIZE)

class PageCache(AbstractClass):
    def __init__(self, file, maxResource):
        super(PageCache, self).__init__(maxResource)
        with open(file, 'ab+') as f:
            f.seek(0, 2)
            length = f.tell()
        f.close()
        self.file = file
        self.fileLock = threading.RLock()
        self.pageNumber = length // PAGE_SIZE

    def newPage(self, initData):                   # 开一个新页
        self.pageNumber += 1
        pgno = self.pageNumber
        pg = Page.Page(pgno, initData, None)
        self.flush(pg)                             # 新建的页面需要立刻写回
        return pgno

    def getPage(self, pgno):
        return super().get(pgno)

    def getForCache(self, key):                    # 根据 pageNumber 从数据库文件中读取页数据,并包裹成Page
        pgno = key
        offset = self.pageOffset(pgno)
        self.fileLock.acquire()
        try:
            with open(self.file, 'rb+') as f:
                f.seek(offset, 0)
                data = f.read(PAGE_SIZE)
        finally:
            self.fileLock.release()
        return Page.Page(pgno, bytearray(data), self)

    def releaseForCache(self, pg):
        if pg.isDirty():
            self.flush(pg)
            pg.setDirty(False)

    def release(self, pg):
        super().release(pg.pageNumber)
#        self.release(pg.getPageNumber())

    def flushPage(self, pg):
        self.flush(pg)

    def flush(self, pg):                           # 页面写入文件系统
        pgno = pg.getPageNumber()
        offset = self.pageOffset(pgno)
        self.fileLock.acquire()
        try:
            with open(self.file, 'rb+') as f:
                f.seek(offset, 0)
                f.write(bytes(pg.getData()))
        finally:
            self.fileLock.release()
            
    def truncateByBgno(self, maxPgno):
        size = self.pageOffset(maxPgno + 1)
        with open(self.file, 'rb+') as f:
            f.truncate(size)
        self.pageNumber = maxPgno

    def getPageNumber(self):
        return self.pageNumber
    
    def pageOffset(self, pgno):
        return (pgno - 1) * PAGE_SIZE
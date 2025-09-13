import threading
from backend.dm.page import Page
from backend.common.AbstractCache import AbstractClass

# 每个页面默认 8kb, 如果需要对大数据的更快速写入, 可以适当增大这个值
PAGE_SIZE = 1 << 13
MEM_MIN_LIM = 10
DB_SUFFIX = '.db'

class PageCache(AbstractClass):
    def __init__(self, file: str, maxResource: int):
        super(PageCache, self).__init__(maxResource)
        with open(file, 'ab+') as f:
            f.seek(0, 2)
            length = f.tell()
        f.close()
        self.file = file
        self.fileLock = threading.RLock()
        # pageNumber 记录当前打开的数据库文件有多少页
        self.pageNumber = length // PAGE_SIZE

    def newPage(self, initData: bytearray | bytes) -> int:
        '''
        开一个新页
        '''
        self.pageNumber += 1
        pgno = self.pageNumber
        pg = Page.Page(pgno, initData, None)
        # 新建的页面需要立刻写回
        self.flush(pg)
        return pgno

    def getPage(self, pgno: int) -> Page.Page:
        return super().get(pgno)

    def getForCache(self, key: int) -> Page.Page:
        '''
        根据 pageNumber 从数据库文件中读取页数据,并包裹成Page
        '''
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

    def releaseForCache(self, pg: Page.Page) -> None:
        '''
        脏页面需要被写回磁盘
        '''
        if pg.isDirty():
            self.flush(pg)
            pg.setDirty(False)

    def release(self, pg):
        super().release(pg.pageNumber)

    def flush(self, pg: Page.Page) -> None:
        '''
        页面写入文件系统
        '''
        pgno = pg.pageNumber
        offset = self.pageOffset(pgno)
        self.fileLock.acquire()
        try:
            with open(self.file, 'rb+') as f:
                f.seek(offset, 0)
                f.write(bytes(pg.data))
        finally:
            self.fileLock.release()
    
    def flushPage(self, pg: Page.Page) -> None:
        self.flush(pg)

    def truncateByBgno(self, maxPgno: int) -> None:
        '''
        把数据文件截断至 maxPgno
        '''
        size = self.pageOffset(maxPgno + 1)
        with open(self.file, 'rb+') as f:
            f.truncate(size)
        self.pageNumber = maxPgno

    def pageOffset(self, pgno: int) -> int:
        '''
        根据 pageNumber 计算偏移量
        '''
        return (pgno - 1) * PAGE_SIZE
    
    def getPageNumber(self) -> int:
        return self.pageNumber
    
def create(path: str, memory: int) -> PageCache:
    file = path + DB_SUFFIX
    return PageCache(file, memory // PAGE_SIZE)

def fileopen(path: str, memory: int) -> PageCache:
    file = path + DB_SUFFIX
    return PageCache(file, memory // PAGE_SIZE)

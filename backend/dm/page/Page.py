# 定义一个页面
# 其中对 pageCache 的引用是为了快速对缓存进行 release 操作
import threading

class Page(object):
    def __init__(self, pageNumber: int, data: bytes | bytearray, pc):
        # pageNumber 是这个页面的页号
        self.pageNumber = pageNumber
        self.data = data
        # dirty 标志着这个页面是否是脏页面, 在缓存驱逐的时候, 脏页面需要被写回磁盘
        self.dirty = False
        self.Lock = threading.RLock()

        self.pc = pc

    def lock(self) -> None:
        self.Lock.acquire()

    def unlock(self) -> None:
        self.Lock.release()

    def release(self) -> None:
        self.pc.release(self)

    def setDirty(self, dirty: bool) -> None:
        self.dirty = dirty

    def isDirty(self) -> None:
        return self.dirty
    
    def getPageNumber(self) -> int:
        return self.pageNumber
    
    def getData(self) -> bytearray | bytes:
        return self.data
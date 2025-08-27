import threading

class Page(object):
    def __init__(self, pageNumber, data, pc):
        self.pageNumber = pageNumber               # 页面数
        self.data = data
        self.dirty = False
        self.Lock = threading.RLock()

        self.pc = pc

    def lock(self):
        self.Lock.acquire()

    def unlock(self):
        self.Lock.release()

    def release(self):
        self.pc.release(self)

    def setDirty(self, dirty):
        self.dirty = dirty

    def isDirty(self):
        return self.dirty
    
    def getPageNumber(self):
        return self.pageNumber
    
    def getData(self):
        return self.data
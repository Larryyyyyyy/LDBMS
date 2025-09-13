# 日志的二进制文件: [XChecksum][Log1][Log2][Log3]...[LogN][BadTail]
# 每个 [Log] 包括 [Size][Checksum][Data], [Size]是一个 4 字节整数
# XChecksum 为所有 Checksum 的校检值求和, 所有 Checksum 都是一个 4 字节整数
# BadTail 是在数据库崩溃时, 没有来得及写完的日志数据
import struct
import threading

SEED = 13331
OF_XCHECKSUM = 4
OF_SIZE = 0
OF_CHECKSUM = OF_SIZE + 4
OF_DATA = OF_CHECKSUM + 4
MASK = 0xFFFFFFFF

LOG_SUFFIX = ".log"

class Logger(object):
    def __init__(self, file: str, xChecksum = 0):
        self.file = file
        # 日志指针的位置
        self.position = 0
        # 日志文件的大小
        self.fileSize = 0
        # 所有Checksum的求和
        self.xChecksum = xChecksum
        self.lock = threading.RLock()
    
    def init(self) -> None:
        '''
        写入 XChecksum
        '''
        with open(self.file, "rb+") as f:
            f.seek(0, 0)
            raw = f.read(4)
            f.seek(0, 2)
            self.fileSize = f.tell()
        f.close()
        self.xChecksum = struct.unpack(">i", raw)[0]
        self.checkAndRemoveTail()

    def checkAndRemoveTail(self) -> None:
        '''
        检查并移除 BadTail
        '''
        self.rewind()
        xCheck = 0
        while True:
            log = self.internNext()
            if log == None:
                break
            xCheck = self.calChecksum(xCheck, log)
        self.truncate(self.position)
        self.rewind()

    def handle_exceed(self, xCheck: int) -> int:
        '''
        对 4 字节整数的手动处理
        '''
        res = xCheck & MASK
        if res & 0x80000000:
            res = res - 0x100000000
        return res

    def calChecksum(self, xCheck: int, log: bytearray) -> int:
        '''
        计算Checksum
        '''
        for i in log:
            xCheck = self.handle_exceed(xCheck * SEED)
            val = i
            if val >= 128:
                val -= 256
            xCheck = self.handle_exceed(xCheck + val)
        return xCheck
    
    def wrapLog(self, data: bytearray | bytes) -> bytearray | bytes:
        '''
        包装一个 [log]
        '''
        checksum = struct.pack(">i", self.calChecksum(0, data))
        size = struct.pack(">i", len(data))
        return size + checksum + data

    def log(self, data: bytearray | bytes) -> None:
        '''
        写入 [log]
        '''
        log = self.wrapLog(data)
        self.lock.acquire()
        try:
            with open(self.file, 'rb+') as f:
                f.seek(0, 2)
                f.write(log)
            f.close()
        finally:
            self.lock.release()
        self.updateXChecksum(log)

    def next(self) -> None | bytearray:
        '''
        读取 position 位置的 [log]
        '''
        self.lock.acquire()
        try:
            log = self.internNext()
            if log == None:
                return None
            return log[OF_DATA : len(log)]
        finally:
            self.lock.release()
    
    def internNext(self) -> None | bytearray:
        '''
        读取 [log] 的具体实现, 同时更新 position
        '''
        if self.position + OF_DATA >= self.fileSize:
            return None
        # 读取size
        with open(self.file, "rb+") as f:
            f.seek(self.position)
            tmp = f.read(4)
        f.close()
        size = struct.unpack(">i", tmp)[0]
        if self.position + OF_DATA + size > self.fileSize:
            return None
        # 读取checkSum + data
        with open(self.file, "rb+") as f:
            f.seek(self.position)
            buf = f.read(OF_DATA + size)
        f.close()
        log = bytearray(buf)
        checkSum1 = self.calChecksum(0, log[OF_DATA : len(log)])
        checkSum2 = struct.unpack(">i", log[OF_CHECKSUM : OF_DATA])[0]
        if checkSum1 != checkSum2:
            return None
        self.position += len(log)
        return log

    def updateXChecksum(self, log: bytearray | bytes) -> None:
        '''
        一条 [log] 变动时, 要修改 XChecksum
        '''
        self.xChecksum = self.calChecksum(self.xChecksum, log)
        with open(self.file, 'rb+') as f:
            f.seek(0, 0)
            f.write(struct.pack(">i", self.xChecksum))
        f.close()

    def truncate(self, x: int) -> None:
        '''
        截断 log 文件前 x 部分
        '''
        self.lock.acquire()
        try:
            with open(self.file, "rb+") as f:
                f.truncate(x)
            f.close()
        finally:
            self.lock.release()

    def rewind(self) -> None:
        self.position = OF_XCHECKSUM

def create(path: str) -> Logger:
    '''
    创建 log 文件
    '''
    fileName = path + LOG_SUFFIX
    with open(fileName, 'wb+') as f:
        f.seek(0, 0)
        f.write(struct.pack('>i', 0))
    f.close()
    return Logger(fileName)

def fileopen(path: str) -> Logger:
    '''
    打开 log 文件
    '''
    fileName = path + LOG_SUFFIX
    lg = Logger(fileName)
    lg.init()
    return lg

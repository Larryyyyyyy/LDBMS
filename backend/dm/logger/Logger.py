# 日志的二进制文件: [XChecksum][Log1][Log2][Log3]...[LogN][BadTail]
# 每个 [Log] 包括 [Size][Checksum][Data], [Size]是一个4字节整数
# XChecksum 为所有 Checksum 的校检值求和,所有 Checksum 都是一个4字节整数
# BadTail 是在数据库崩溃时,没有来得及写完的日志数据
import struct
import threading

SEED = 13331
OF_SIZE = 0
OF_CHECKSUM = OF_SIZE + 4
OF_DATA = OF_CHECKSUM + 4
MASK = 0xFFFFFFFF

LOG_SUFFIX = ".log"

def create(path):
    fileName = path + LOG_SUFFIX
    with open(fileName, 'wb+') as f:
        f.seek(0, 0)
        f.write(struct.pack('>i', 0))
    f.close()
    return Logger(fileName)

def fileopen(path):
    fileName = path + LOG_SUFFIX
    lg = Logger(fileName)
    lg.init()
    return lg

class Logger(object):
    def __init__(self, file, xChecksum = 0):
        self.file = file
        self.position = 0                          # 日志指针的位置
        self.fileSize = 0                          # 日志文件的大小
        self.xChecksum = xChecksum                 # 所有Checksum的求和
        self.lock = threading.RLock()
    
    def init(self):
        with open(self.file, "rb+") as f:          # 写入 XChecksum
            f.seek(0, 0)
            raw = f.read(4)
            f.seek(0, 2)
            self.fileSize = f.tell()
        f.close()
        self.xChecksum = struct.unpack(">i", raw)[0]
        self.checkAndRemoveTail()

    def checkAndRemoveTail(self):                  # 检查并移除 BadTail
        self.rewind()
        xCheck = 0
        while True:
            log = self.internNext()
            if log == None:
                break
            xCheck = self.calChecksum(xCheck, log)
        self.truncate(self.position)
        self.rewind()

    def handle_exceed(self, xCheck):
        res = xCheck & MASK
        if res & 0x80000000:
            res = res - 0x100000000
        return res

    def calChecksum(self, xCheck, log):            # 计算Checksum
        for i in log:
            xCheck = self.handle_exceed(xCheck * SEED)
            val = i
            if val >= 128:
                val -= 256
            xCheck = self.handle_exceed(xCheck + val)
        return xCheck
    
    def log(self, data):
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

    def internNext(self):
        if self.position + OF_DATA >= self.fileSize:
            return None
        with open(self.file, "rb+") as f:          # 读取size
            f.seek(self.position)
            tmp = f.read(4)
        f.close()
        size = struct.unpack(">i", tmp)[0]
        if self.position + OF_DATA + size > self.fileSize:
            return None
        with open(self.file, "rb+") as f:          # 读取checkSum + data
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

    def updateXChecksum(self, log):
        self.xChecksum = self.calChecksum(self.xChecksum, log)
        with open(self.file, 'rb+') as f:
            f.seek(0, 0)
            f.write(struct.pack(">i", self.xChecksum))
        f.close()

    def wrapLog(self, data):
        checksum = struct.pack(">i", self.calChecksum(0, data))
        size = struct.pack(">i", len(data))
        return size + checksum + data

    def truncate(self, x):
        self.lock.acquire()
        try:
            with open(self.file, "rb+") as f:
                f.truncate(x)
            f.close()
        finally:
            self.lock.release()

    def next(self):
        self.lock.acquire()
        try:
            log = self.internNext()
            if log == None:
                return None
            return log[OF_DATA : len(log)]
        finally:
            self.lock.release()
    
    def rewind(self):
        self.position = 4

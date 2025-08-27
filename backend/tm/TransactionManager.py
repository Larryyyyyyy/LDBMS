import struct
import threading

LEN_XID_HEADER_LENGTH = 8                          # XID 文件头长度
XID_FIELD_SIZE = 1                                 # 每个事务占用长度

FIELD_TRAN_ACTIVE = 0                              # 事务的三种状态
FIELD_TRAN_COMMITTED = 1
FIELD_TRAN_ABORTED = 2

SUPER_XID = 0                                      # 超级事务的 XID
XID_SUFFIX = '.xid'                                # 文件后缀

def create(path):
    with open(path + XID_SUFFIX, 'wb+') as f:
        f.write(struct.pack('>q', 0))
    return TransactionManager(path + XID_SUFFIX)

def fileopen(path):
    return TransactionManager(path + XID_SUFFIX)

class TransactionManager(object):
    def __init__(self, raf):
        self.file = raf                            # 文件
        self.xidCounter = 0                        # 事务数目
        self.counterLock = threading.RLock()
        self.checkXIDCounter()

    def checkXIDCounter(self):                     # 检验 xid 文件是否合法
        with open(self.file, 'rb') as f:
            f.seek(0, 0)
            header = f.read(8)
            expected_length = struct.unpack('>Q', header)[0] + 8
            f.seek(0, 2)
            file_size = f.tell()
            if expected_length != file_size:
                exit()
        f.close()
        self.xidCounter = file_size - 8
    
    def getXidPosition(self, xid):                 # 根据事务 xid 读取 xid 文件对应的位置
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE
    
    def updateXID(self, xid, status):              # xid 文件更新新的事务状态
        offset = self.getXidPosition(xid)
        tmp = struct.pack('B', status)
        with open(self.file, 'rb+') as f:
            f.seek(offset, 0)
            f.write(tmp)
        f.close()

    def incrXIDCounter(self):                      # 自增 xidCounter 同时修改 xid 文件
        self.xidCounter += 1
        with open(self.file, 'rb+') as f:
            f.seek(0, 0)
            f.write(struct.pack('>q', self.xidCounter))
        f.close()

    def checkXID(self, xid, status):               # 检查 xid 的事务是否处于 status 状态
        offset = self.getXidPosition(xid)
        with open(self.file, 'rb') as f:
            f.seek(offset, 0)
            rec = f.read(XID_FIELD_SIZE)
        f.close()
        return struct.unpack('B', rec)[0] == status

    def begin(self):                               # 开启一个新事务
        self.counterLock.acquire()
        try:
            xid = self.xidCounter + 1
            self.updateXID(xid, FIELD_TRAN_ACTIVE)
            self.incrXIDCounter()
        finally:
            self.counterLock.release()
        return xid

    def commit(self, xid):                         # 提交一个事务
        self.updateXID(xid, FIELD_TRAN_COMMITTED)

    def abort(self, xid):                          # 取消一个事务
        self.updateXID(xid, FIELD_TRAN_ABORTED)

    def isActive(self, xid):                       # 查询一个事务的状态是否是正在进行的状态
        if xid == SUPER_XID:
            return False
        else:
            return self.checkXID(xid, FIELD_TRAN_ACTIVE)

    def isCommitted(self, xid):                    # 查询一个事务的状态是否是已经提交的状态
        if xid == SUPER_XID:
            return True
        else:
            return self.checkXID(xid, FIELD_TRAN_COMMITTED)
        
    def isAborted(self, xid):                      # 查询一个事务的状态是否是已取消
        if xid == SUPER_XID:
            return False
        else:
            return self.checkXID(xid, FIELD_TRAN_ABORTED)

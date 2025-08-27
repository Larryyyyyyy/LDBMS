'''
三个操作:可见性判断,获取资源的锁,版本跳跃判断
删除的操作只有一个设置 XMAX
'''
import threading
import backend.tm
import backend.tm.TransactionManager
import backend.vm.Entry
import backend.vm.Visibility
from backend.vm.Transaction import newTransaction
from backend.vm.LockTable import LockTable
from backend.common.AbstractCache import AbstractClass

def newVersionManager(tm, dm):
    return VersionManager(tm, dm)

class VersionManager(AbstractClass):
    def __init__(self, tm, dm):
        super().__init__(0)
        self.tm = tm
        self.dm = dm
        self.activeTransaction = {}
        self.activeTransaction[backend.tm.TransactionManager.SUPER_XID] = newTransaction(backend.tm.TransactionManager.SUPER_XID, 0, None)
        self.lock = threading.RLock()
        self.lt = LockTable()

    '''
    开启一个事务并初始化事务的结构
    将其存放在 activeTransaction 中,用于检查和快照使用
    '''
    def begin(self, level):
        self.lock.acquire()
        try:
            xid  = self.tm.begin()
            t = newTransaction(xid, level, self.activeTransaction)
            self.activeTransaction[xid] = t
            return xid
        finally:
            self.lock.release()

    '''
    方法提交一个事务,就是 free 掉相关的结构,并且释放持有的锁,并修改 TM 状态
    '''
    def commit(self, xid):
        self.lock.acquire()
        t = self.activeTransaction[xid]
        self.lock.release()
        if t.err != None:
            raise t.err
        self.lock.acquire()
        self.activeTransaction.pop(xid, None)
        self.lock.release()
        self.lt.remove(xid)
        self.tm.commit(xid)

    '''
    注意判断 entry 对事务的可见性
    '''
    def read(self, xid, uid):
        self.lock.acquire()
        t = self.activeTransaction[xid]
        self.lock.release()
        if t.err != None:
            raise t.err
        entry = None
        try:
            entry = super().get(uid)
        except Exception as e:
            if e == Exception("NullEntryException"):
                return None
            else:
                raise e
        try:
            if backend.vm.Visibility.isVisible(self.tm, t, entry) == True:
                return entry.data()
            else:
                return None
        finally:
            entry.release()

    '''
    把数据包裹成 Entry 交给 DataManager
    '''
    def insert(self, xid, data):
        self.lock.acquire()
        t = self.activeTransaction[xid]
        self.lock.release()
        if t.err != None:
            raise t.err
        raw = backend.vm.Entry.wrapEntryRaw(xid, data)
        return self.dm.insert(xid, raw)

    def delete(self, xid, uid):
        self.lock.acquire()
        t = self.activeTransaction[xid]
        self.lock.release()
        if t.err != None:
            raise t.err
        entry = None
        try:
            entry = super().get(uid)
        except Exception as e:
            if e == Exception("NullEntryException"):
                return False
            else:
                raise e
        try:
            if not backend.vm.Visibility.isVisible(self.tm, t, entry):
                return False
            l = None
            try:
                l = self.lt.add(xid, uid)
            except Exception as e:
                t.err = Exception("ConcurrentUpdateException")
                self.internAbort(xid, True)
                t.autoAborted = True
                raise t.err
            if l != None:
                l.acquire()
                l.release()
            if entry.getXmax() == xid:
                return False
            if backend.vm.Visibility.isVeresionSkip(self.tm, t, entry):
                t.err = Exception("ConcurrentUpdateException")
                self.internAbort(xid, True)
                t.autoAborted = True
                raise t.err
            entry.setXmax(xid)
            return True
        finally:
            entry.release()
            
    '''
    abort 事务的方法则有两种:手动和自动
    手动指的是调用 abort() 方法
    自动是在事务被检测出出现死锁时,会自动撤销回滚事务;或者出现版本跳跃时,也会自动回滚
    '''
    def abort(self, xid):
        self.internAbort(xid, False)

    def internAbort(self, xid, autoAborted):
        self.lock.acquire()
        t = self.activeTransaction[xid]
        if autoAborted == False:
            self.activeTransaction.pop(xid, None)
        self.lock.release()
        if t.autoAborted == True:
            return
        self.lt.remove(xid)
        self.tm.abort(xid)

    def releaseEntry(self, entry):
        super().release(entry.uid)

    def getForCache(self, uid):
        entry = backend.vm.Entry.loadEntry(self, uid)
        if entry == None:
            raise Exception("NullEntryException")
        return entry

    def releaseForCache(self, entry):
        entry.remove()

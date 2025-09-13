'''
三个操作:可见性判断,获取资源的锁,版本跳跃判断
删除的操作只有一个设置 XMAX
'''
import threading
import backend.tm
import backend.tm.TransactionManager
import backend.vm.Entry
import backend.vm.Visibility
from backend.tm.TransactionManager import TransactionManager
from backend.dm.DataManager import DataManager
from backend.vm.Transaction import newTransaction
from backend.vm.LockTable import LockTable
from backend.vm.Entry import Entry
from backend.common.AbstractCache import AbstractClass

class VersionManager(AbstractClass):
    def __init__(self, tm: TransactionManager, dm: DataManager):
        super().__init__(0)
        self.tm = tm
        self.dm = dm
        self.activeTransaction = {}
        self.activeTransaction[backend.tm.TransactionManager.SUPER_XID] = newTransaction(backend.tm.TransactionManager.SUPER_XID, 0, None)
        self.lock = threading.RLock()
        self.lt = LockTable()

    def begin(self, level: int) -> int:
        '''
        开启一个事务并初始化事务的结构
        将其存放在 activeTransaction 中, 用于检查和快照使用
        '''
        self.lock.acquire()
        try:
            xid = self.tm.begin()
            t = newTransaction(xid, level, self.activeTransaction)
            self.activeTransaction[xid] = t
            return xid
        finally:
            self.lock.release()

    def commit(self, xid: int) -> None:
        '''
        方法提交一个事务, 就是 free 掉相关的结构, 并且释放持有的锁, 并修改 TM 状态
        '''
        self.lock.acquire()
        t = self.activeTransaction[xid]
        self.lock.release()
        if t.err != None:
            raise t.err
        self.lock.acquire()
        self.activeTransaction.pop(xid, None)
        self.lock.release()
        # 清理该事务持有的所有锁
        self.lt.remove(xid)
        # 通知事务管理器事务已提交
        self.tm.commit(xid)

    def read(self, xid: int, uid: int) -> None | bytearray | bytes:
        '''
        读取并判断 entry 对事务的可见性
        '''
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

    def insert(self, xid: int, data: bytearray | bytes) -> int:
        '''
        把数据包裹成 Entry 交给 DataManager 完成插入
        '''
        self.lock.acquire()
        t = self.activeTransaction[xid]
        self.lock.release()
        if t.err != None:
            raise t.err
        raw = backend.vm.Entry.wrapEntryRaw(xid, data)
        return self.dm.insert(xid, raw)

    def delete(self, xid: int, uid: int) -> bool:
        '''
        删除指定 uid 的数据
        '''
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
            
    def abort(self, xid: int) -> None:
        '''
        手动 abort 事务
        '''
        self.internAbort(xid, False)

    def internAbort(self, xid: int, autoAborted: bool) -> None:
        '''
        autoAborted: 1 自动 abort 事务
        autoAborted: 0 手动 abort 事务
        在事务被检测出出现死锁时, 会自动撤销回滚事务; 或者出现版本跳跃时, 也会自动回滚
        '''
        self.lock.acquire()
        t = self.activeTransaction[xid]
        if autoAborted == False:
            self.activeTransaction.pop(xid, None)
        self.lock.release()
        # 事务不从 activeTransaction 中移除, 因为这可能影响其他事务的快照
        if t.autoAborted == True:
            return
        self.lt.remove(xid)
        self.tm.abort(xid)

    def releaseEntry(self, entry):
        super().release(entry.uid)

    def getForCache(self, uid: int) -> Entry:
        entry = backend.vm.Entry.loadEntry(self, uid)
        if entry == None:
            raise Exception("NullEntryException")
        return entry

    def releaseForCache(self, entry):
        entry.remove()

def newVersionManager(tm: TransactionManager, dm: DataManager) -> VersionManager:
    return VersionManager(tm, dm)

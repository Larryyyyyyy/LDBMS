# vm对一个事务的抽象
from backend.tm import TransactionManager

class Transaction(object):
    def __init__(self, xid: int, level: int, active: dict | None):
        self.xid = xid
        self.level = level
        # 快照: 该事务在整个生命周期中能够看到的事务
        self.snapshot = {}
        self.err = None
        self.autoAborted = False
        if level != 0:
            for x in active:
                self.snapshot[x] = True

    def isInSnapshot(self, xid: int) -> bool:
        if xid == TransactionManager.SUPER_XID:
            return False
        return self.snapshot.get(xid) != None

def newTransaction(xid: int, level: int, active: bool) -> Transaction:
    return Transaction(xid, level, active)

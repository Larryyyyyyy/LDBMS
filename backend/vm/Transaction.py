'''
vm对一个事务的抽象
'''
from backend.tm import TransactionManager

def newTransaction(xid, level, active):
    return Transaction(xid, level, active)

class Transaction(object):
    def __init__(self, xid, level, active):
        self.xid = xid
        self.level = level
        self.snapshot = {}
        self.err = None
        self.autoAborted = False
        if level != 0:
            for x in active:
                self.snapshot[x] = True

    def isInSnapshot(self, xid):
        if xid == TransactionManager.SUPER_XID:
            return False
        return self.snapshot.get(xid) != None

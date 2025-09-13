'''
2PL 会阻塞事务,直至持有锁的线程释放锁
可以将这种等待关系抽象成有向边
例如 Tj 在等待 Ti, 就可以表示为 Tj —> Ti
无数有向边就可以形成一个图
检测死锁只需要查看这个图中是否有环即可
'''
import threading

class LockTable(object):
    def __init__(self):
        # 某个 XID 已经获得的资源的 UID 列表
        self.x2u = {}
        # UID 被某个 XID 持有
        self.u2x = {}
        # 正在等待 UID 的 XID 列表
        self.wait = {}
        # 正在等待资源的 XID 的锁
        self.waitLock = {}
        # XID 正在等待的 UID
        self.waitU = {}
        self.lock = threading.RLock()
        self.xidStamp = {}
        self.stamp = 0

    def add(self, xid: int, uid: int) -> None | threading._RLock:
        '''
        在每次出现等待的情况时, 就尝试向图中增加一条边, 并进行死锁检测
        如果检测到死锁, 就撤销这条边, 不允许添加, 并撤销该事务
        '''
        self.lock.acquire()
        try:
            if self.isInList(self.x2u, xid, uid):
                return None
            if self.u2x.get(uid) == None:
                self.u2x[uid] = xid
                self.putIntoList(self.x2u, xid, uid)
                return None
            self.waitU[xid] = uid
            self.putIntoList(self.wait, uid, xid)
            if self.hasDeadLock():
                self.waitU.pop(xid, None)
                self.removeFromList(self.wait, uid, xid)
                raise Exception("DeadlockException")
            l = threading.RLock()
            l.acquire()
            self.waitLock[xid] = l
            return l
        finally:
            self.lock.release()
    
    def remove(self, xid: int) -> None:
        '''
        在一个事务 commit 或者 abort 时, 就可以释放所有它持有的锁, 并将自身从等待图中删除
        '''
        self.lock.acquire()
        try:
            l = self.x2u.get(xid)
            if l != None:
                while len(l) > 0:
                    uid = l.pop(0)
                    self.selectNewXID(uid)
            self.waitU.pop(xid, None)
            self.x2u.pop(xid, None)
            self.waitLock.pop(xid, None)
        finally:
            self.lock.release()

    def selectNewXID(self, uid: int) -> None:
        '''
        从等待队列中选择一个 xid 来占用 uid
        '''
        self.u2x.pop(uid, None)
        l = self.wait.get(uid)
        if l == None:
            return
        assert len(l) > 0
        while len(l) > 0:
            xid = l.pop(0)
            if self.waitLock.get(xid) != None:
                continue
            else:
                self.u2x[uid] = xid
                lo = self.waitLock.pop(xid, None)
                self.waitU.pop(xid, None)
                lo.release()
                break
        if len(l) == 0:
            self.wait.pop(uid, None)

    def hasDeadLock(self) -> bool:
        '''
        深搜判断环来判断死锁
        '''
        self.xidStamp = {}
        self.stamp = 1
        for xid in self.x2u:
            s = self.xidStamp.get(xid)
            if s != None and s > 0:
                continue
            self.stamp += 1
            if self.dfs(xid):
                return True
        return False
    
    def dfs(self, xid: int) -> bool:
        stp = self.xidStamp.get(xid)
        if stp != None and stp == self.stamp:
            return True
        if stp != None and stp < self.stamp:
            return False
        self.xidStamp[xid] = self.stamp
        uid = self.waitU.get(xid)
        if uid == None:
            return False
        x = self.u2x.get(uid)
        return self.dfs(x)

    def removeFromList(self, listMap: dict, uid0: int, uid1: int) -> None:
        l = listMap[uid0]
        if l == None:
            return
        for i in l:
            if i == uid1:
                l.remove(i)
                break
        if len(l) == 0:
            listMap.pop(uid0)

    def putIntoList(self, listMap: dict, uid0: int, uid1: int) -> None:
        if listMap.get(uid0) == None:
            listMap[uid0] = []
        listMap[uid0].insert(0, uid1)

    def isInList(self, listMap: dict, uid0: int, uid1: int) -> bool:
        l = listMap.get(uid0)
        if l == None:
            return False
        for i in l:
            if i == uid1:
                return True
        return False
    
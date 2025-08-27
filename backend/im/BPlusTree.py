import struct
import threading
import backend.im.Node
import backend.tm.TransactionManager

def create(dm):
    rawRoot = backend.im.Node.newNilRootRaw()
    rootUid = dm.insert(backend.tm.TransactionManager.SUPER_XID, rawRoot)
    return dm.insert(backend.tm.TransactionManager.SUPER_XID, struct.pack(">q", rootUid))

def load(bootUid, dm):
    bootDataItem = dm.read(bootUid)
    assert bootDataItem != None
    t = BPlusTree(dm, bootUid, bootDataItem, threading.RLock())
    return t

class BPlusTree(object):
    def __init__(self, dm, bootUid, bootDataItem, bootLock):
        self.dm = dm
        self.bootUid = bootUid
        self.bootDataItem = bootDataItem
        self.bootLock = bootLock

    class InsertRes(object):
        def __init__(self, newNode = 0, newKey = 0):
            self.newNode = newNode
            self.newKey = newKey

    def rootUid(self):
        self.bootLock.acquire()
        try:
            sa = self.bootDataItem.data()
            return struct.unpack('>q', sa.raw[sa.start : sa.start + 8])[0]
        finally:
            self.bootLock.release()

    def updateRootUid(self, left, right, rightKey):
        self.bootLock.acquire()
        try:
            rootRaw = backend.im.Node.newRootRaw(left, right, rightKey)
            newRootUid = self.dm.insert(backend.tm.TransactionManager.SUPER_XID, rootRaw)
            self.bootDataItem.before()
            self.diRaw = self.bootDataItem.data()
            self.diRaw[self.diRaw.start : self.diRaw.start + 8] = struct.pack('>q', newRootUid)
            self.bootDataItem.after(backend.tm.TransactionManager.SUPER_XID)
        finally:
            self.bootLock.release()

    def searchLeaf(self, nodeUid, key):
        node = backend.im.Node.loadNode(self, nodeUid)
        isLeaf = node.isLeaf()
        node.release()
        if isLeaf == True:
            return nodeUid
        else:
            next = self.searchNext(nodeUid, key)
            return self.searchLeaf(next, key)

    def searchNext(self, nodeUid, key):
        while True:
            node = backend.im.Node.loadNode(self, nodeUid)
            res = node.searchNext(key)
            node.release()
            if res.uid != 0:
                return res.uid
            nodeUid = res.siblingUid

    def search(self, key):
        return self.searchRange(key, key)

    def searchRange(self, leftKey, rightKey):
        rootUid = self.rootUid()
        leafUid = self.searchLeaf(rootUid, leftKey)
        uids = []
        while True:
            leaf = backend.im.Node.loadNode(self, leafUid)
            res = leaf.leafSearchRange(leftKey, rightKey)
            leaf.release()
            for i in res.uids:
                uids.append(i)
            if res.siblingUid == 0:
                break
            else:
                leafUid = res.siblingUid
        return uids

    def insert(self, key, uid):
        rootUid = self.rootUid()
        res = self.insert_Res(rootUid, uid, key)
        if res.newNode != 0:
            self.updateRootUid(rootUid, res.newNode, res.newKey)

    def insert_Res(self, nodeUid, uid, key):
        node = backend.im.Node.loadNode(self, nodeUid)
        isLeaf = node.isLeaf()
        node.release()
        res = None
        if isLeaf == True:
            res = self.insertAndSplit(nodeUid, uid, key)
        else:
            next = self.searchNext(nodeUid, key)
            ir = self.insert_Res(next, uid, key)
            if ir.newNode != 0:
                res = self.insertAndSplit(nodeUid, ir.newNode, ir.newKey)
            else:
                res = self.InsertRes()
        return res
    def insertAndSplit(self, nodeUid, uid, key):
        while True:
            node = backend.im.Node.loadNode(self, nodeUid)
            iasr = node.insertAndSplit(uid, key)
            node.release()
            if iasr.siblingUid != 0:
                nodeUid = iasr.siblingUid
            else:
                res = self.InsertRes(iasr.newSon, iasr.newKey)
                return res

    def close(self):
        self.bootDataItem.release()

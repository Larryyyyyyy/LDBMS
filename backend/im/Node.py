'''
二叉树由一个个 Node 组成，每个 Node 都存储在一条 DataItem 中,结构如下:
[LeafFlag][KeyNumber][SiblingUid]
[Son0][Key0][Son1][Key1]...[SonN][KeyN]
其中 LeafFlag 标记了该节点是否是个叶子节点;
KeyNumber 为该节点中 key 的个数;
SiblingUid 是其兄弟节点存储在 DM 中的 UID
后续是穿插的子节点(SonN)和 KeyN
最后的一个 KeyN 始终为 MAX_VALUE 以此方便查找。

Node 类持有了其 B+ 树结构的引用,DataItem 的引用和 SubArray 的引用,用于方便快速修改数据和释放数据
'''
import struct
import backend.tm.TransactionManager
from backend.common.SubArray import SubArray
from backend.dm.dataItem import DataItem
from backend.tm.TransactionManager import TransactionManager

IS_LEAF_OFFSET = 0
NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1
SIBLING_OFFSET = NO_KEYS_OFFSET + 2
NODE_HEADER_SIZE = SIBLING_OFFSET + 8
BALANCE_NUMBER = 32
NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2)

def setRawIsLeaf(raw, isLeaf):
    if isLeaf:
        raw.raw[raw.start + IS_LEAF_OFFSET] = 1
    else:
        raw.raw[raw.start + IS_LEAF_OFFSET] = 0

def getRawIfLeaf(raw):
    return raw.raw[raw.start + IS_LEAF_OFFSET] == 1

def setRawNoKeys(raw, noKeys):
    raw.raw[raw.start + NO_KEYS_OFFSET : raw.start + NO_KEYS_OFFSET + 2] = struct.pack('>h', noKeys)

def getRawNoKeys(raw):
    return struct.unpack('>h', raw.raw[raw.start + NO_KEYS_OFFSET : raw.start + NO_KEYS_OFFSET + 2])[0]

def setRawSibling(raw, sibling):
    raw.raw[raw.start + SIBLING_OFFSET : raw.start + SIBLING_OFFSET + 8] = struct.pack('>q', sibling)

def getRawSibling(raw):
    return struct.unpack('>q', raw.raw[raw.start + SIBLING_OFFSET : raw.start + SIBLING_OFFSET + 8])[0]

def setRawKthSon(raw, uid, kth):
    offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2)
    raw.raw[offset : offset + 8] = struct.pack('>q', uid)

def getRawKthSon(raw, kth):
    offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2)
    return struct.unpack('>q', raw.raw[offset : offset + 8])[0]

def setRawKthKey(raw, key, kth):
    offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8
    raw.raw[offset : offset + 8] = struct.pack('>q', key)

def getRawKthKey(raw, kth):
    offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8
    return struct.unpack('>q', raw.raw[offset : offset + 8])[0]

def copyRawFromKth(raw, to, kth):
    offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2)
    to.raw[to.start + NODE_HEADER_SIZE : to.start + NODE_HEADER_SIZE + raw.end - offset] = raw.raw[offset : raw.end]

def shiftRawKth(raw, kth):
    begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2)
    end = raw.start + NODE_SIZE - 1
    for i in range(end, begin - 1, -1):
        raw.raw[i] = raw.raw[i - (8 * 2)]

def newRootRaw(left, right, key):
    raw = SubArray(bytearray(NODE_SIZE), 0, NODE_SIZE)
    setRawIsLeaf(raw, False)
    setRawNoKeys(raw, 2)
    setRawSibling(raw, 0)
    setRawKthSon(raw, left, 0)
    setRawKthKey(raw, key, 0)
    setRawKthSon(raw, right, 1)
    setRawKthKey(raw, 9223372036854775807, 1)
    return raw.raw

def newNilRootRaw():
    raw = SubArray(bytearray(NODE_SIZE), 0, NODE_SIZE)
    setRawIsLeaf(raw, True)
    setRawNoKeys(raw, 0)
    setRawSibling(raw, 0)
    return raw.raw

def loadNode(bTree, uid):
    di = bTree.dm.read(uid)
    n = Node()
    n.tree = bTree
    n.dataItem = di
    n.raw = di.data()
    n.uid = uid
    return n

class Node(object):
    def __init__(self, tree = None, dataItem = None, raw = None, uid = None):
        self.tree = tree
        self.dataItem = dataItem
        self.raw = raw
        self.uid = uid

    class SearchNextRes(object):
        def __init__(self, uid = 0, siblingUid = 0):
            self.uid = uid
            self.siblingUid = siblingUid

    class LeafSearchRangeRes(object):
        def __init__(self, uids = [], siblingUid = 0):
            self.uids = uids
            self.siblingUid = siblingUid

    class InsertAndSplitRes(object):
        def __init__(self, siblingUid = 0, newSon = 0, newKey = 0):
            self.siblingUid = siblingUid
            self.newSon = newSon
            self.newKey = newKey

    class SplitRes(object):
        def __init__(self, newSon = 0, newKey = 0):
            self.newSon = newSon
            self.newKey = newKey

    def release(self):
        self.dataItem.release()

    def isLeaf(self):
        self.dataItem.rLock.acquire()
        try:
            return getRawIfLeaf(self.raw)
        finally:
            self.dataItem.rLock.release()

    def searchNext(self, key):
        self.dataItem.rLock.acquire()
        try:
            res = self.SearchNextRes()
            noKeys = getRawNoKeys(self.raw)
            for i in range(noKeys):
                ik = getRawKthKey(self.raw, i)
                if key < ik:
                    res.uid = getRawKthSon(self.raw, i)
                    res.siblingUid = 0
                    return res
            res.uid = 0
            res.siblingUid = getRawSibling(self.raw)
            return res
        finally:
            self.dataItem.rLock.release()
    
    def leafSearchRange(self, leftKey, rightKey):
        self.dataItem.rLock.acquire()
        try:
            noKeys = getRawNoKeys(self.raw)
            kth = 0
            while kth < noKeys:
                ik = getRawKthKey(self.raw, kth)
                if ik >= leftKey:
                    break
                kth += 1
            uids = []
            while kth < noKeys:
                ik = getRawKthKey(self.raw, kth)
                if ik <= rightKey:
                    uids.append(getRawKthSon(self.raw, kth))
                    kth += 1
                else:
                    break
            siblingUid = 0
            if kth == noKeys:
                siblingUid = getRawSibling(self.raw)
            res = self.LeafSearchRangeRes()
            res.uids = uids
            res.siblingUid = siblingUid
            return res
        finally:
            self.dataItem.rLock.release()

    def insertAndSplit(self, uid, key):
        success = False
        res = self.InsertAndSplitRes()
        self.dataItem.before()
        try:
            success =self.insert(uid, key)
            if success == False:
                res.siblingUid = getRawSibling(self.raw)
                return res
            if self.needSplit() == True:
                r = self.split()
                res.newSon = r.newSon
                res.newKey = r.newKey
                return res
            else:
                return res
        finally:
            if success == True:
                self.dataItem.after(backend.tm.TransactionManager.SUPER_XID)
            else:
                self.dataItem.unBefore()

    def insert(self, uid, key):
        noKeys = getRawNoKeys(self.raw)
        kth = 0
        while kth < noKeys:
            ik = getRawKthKey(self.raw, kth)
            if ik < key:
                kth += 1
            else:
                break
        if kth == noKeys and getRawSibling(self.raw) != 0:
            return False
        if getRawIfLeaf(self.raw) == True:
            shiftRawKth(self.raw, kth)
            setRawKthKey(self.raw, key, kth)
            setRawKthSon(self.raw, uid, kth)
            setRawNoKeys(self.raw, noKeys + 1)
        else:
            kk = getRawKthKey(self.raw, kth)
            setRawKthKey(self.raw, key, kth)
            shiftRawKth(self.raw, kth + 1)
            setRawKthKey(self.raw, kk, kth + 1)
            setRawKthSon(self.raw, uid, kth + 1)
            setRawNoKeys(self.raw, noKeys + 1)
        return True

    def needSplit(self):
        return BALANCE_NUMBER * 2 == getRawNoKeys(self.raw)

    def split(self):
        nodeRaw = SubArray(bytearray(NODE_SIZE), 0, NODE_SIZE)
        setRawIsLeaf(nodeRaw, getRawIfLeaf(self.raw))
        setRawNoKeys(nodeRaw, BALANCE_NUMBER)
        setRawSibling(nodeRaw, getRawSibling(self.raw))
        copyRawFromKth(self.raw, nodeRaw, BALANCE_NUMBER)
        son = self.tree.dm.insert(backend.tm.TransactionManager.SUPER_XID, nodeRaw.raw)
        setRawNoKeys(self.raw, BALANCE_NUMBER)
        setRawSibling(self.raw, son)
        res = self.SplitRes(son, getRawKthKey(nodeRaw, 0))
        return res

    def toString(self):
        s = ""
        s += "Is leaf: " + ("True" if getRawIfLeaf(self.raw) else "False") + "\n"
        KeyNumber = getRawNoKeys(self.raw)
        s += "KeyNumber: " + str(KeyNumber) + "\n"
        s += "sibling: " + str(getRawSibling(self.raw)) + "\n"
        for i in range(KeyNumber):
            s += "son: " + str(getRawKthSon(self.raw, i)) + ", key: " + str(getRawKthKey(self.raw, i)) + "\n"
        return s
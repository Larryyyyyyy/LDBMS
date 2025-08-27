import struct
import backend.im.BPlusTree
import backend.tm.TransactionManager
'''
field 表示字段信息
二进制格式为：
[FieldName][TypeName][IndexUid]
如果 field 无索引, IndexUid 为0
'''
class FieldCalRes(object):
    def __init__(self, left = 0, right = 0):
        self.left = left
        self.right = right

class ParseValueRes:
    def __init__(self, v = None, shift = 0):
        self.v = v
        self.shift = shift

def loadField(tb, uid):
    raw = None
    try:
        raw = tb.tbm.vm.read(backend.tm.TransactionManager.SUPER_XID, uid)
    except Exception as e:
        raise e
    assert raw is not None
    return Field(uid, tb).parseSelf(raw)
    
def createField(tb, xid, fieldName, fieldType, indexed):
    typeCheck(fieldType)
    f = Field(0, tb, fieldName, fieldType, 0)
    if indexed:
        index = backend.im.BPlusTree.create(tb.tbm.dm)
        bt = backend.im.BPlusTree.load(index, tb.tbm.dm)
        f.index = index
        f.bt = bt
    f.persistSelf(xid)
    return f

def typeCheck(fieldType):
    if fieldType not in ["int32", "int64", "string"]:
        raise Exception("InvalidFieldException")

class Field:
    def __init__(self, uid = 0, tb = None, fieldName = "", fieldType = "", index = 0):
        self.uid = uid
        self.tb = tb
        self.fieldName = fieldName
        self.fieldType = fieldType
        self.index = index
        self.bt = None

    def parseSelf(self, raw):
        position = 0
        length = struct.unpack('>i', raw[0 : 4])[0]
        s = raw[4 : 4 + length].decode('utf-8')
        position += length + 4
        self.fieldName = s
        length = struct.unpack('>i', raw[position : position + 4])[0]
        s = raw[position + 4 : position + 4 + length].decode('utf-8')
        position += length + 4
        self.fieldType = s
        self.index = struct.unpack('>q', raw[position : position + 8])[0]
        if self.index != 0:
            try:
                self.bt = backend.im.BPlusTree.load(self.index, self.tb.tbm.dm)
            except Exception as e:
                raise e
        return self

    def persistSelf(self, xid):
        nameRaw = struct.pack('>i', len(self.fieldName)) + self.fieldName.encode('utf-8')
        typeRaw = struct.pack('>i', len(self.fieldType)) + self.fieldType.encode('utf-8')
        indexRaw = struct.pack('>q', self.index)
        self.uid = self.tb.tbm.vm.insert(xid, nameRaw + typeRaw + indexRaw)

    def isIndexed(self):
        return self.index != 0
    
    def insert(self, key, uid):
        ukey = self.value2Uid(key)
        self.bt.insert(ukey, uid)

    def search(self, left, right):
        return self.bt.searchRange(left, right)
    
    def value2Uid(self, key):
        uid = 0
        if self.fieldType == "string":
            s = self.fieldType.encode('utf-8')
            for b in s:
                uid = uid * 13331 + b
        elif self.fieldType == "int32":
            uint = int(key)
            return uint
        elif self.fieldType == "int64":
            uid = int(key)
        return uid
    
    def string2Value(self, s):
        if self.fieldType == "int32":
            return int(s)
        elif self.fieldType == "int64":
            return int(s)   
        elif self.fieldType == "string":
            return s
        
    def value2Raw(self, v):
        raw = None
        if self.fieldType == "int32":
            raw = struct.pack('>i', v)
        elif self.fieldType == "int64":
            raw = struct.pack('>q', v)
        elif self.fieldType == "string":
            raw = struct.pack('>i', len(v)) + v.encode('utf-8')
        return raw
    
    def parseValue(self, raw):
        res = ParseValueRes()
        if self.fieldType == "int32":
            res.v = struct.unpack('>i', raw[0 : 4])[0]
            res.shift = 4
        elif self.fieldType == "int64":
            res.v = struct.unpack('>q', raw[0 : 8])[0]
            res.shift = 8
        elif self.fieldType == "string":
            length = struct.unpack('>i', raw[0 : 4])[0]
            res.v = str(raw[4 : 4 + length])
            res.shift = length + 4
        return res
    
    def printValue(self, v):
        s = None
        if self.fieldType == "int32":
            s = str(v)
        elif self.fieldType == "int64":
            s = str(v)
        elif self.fieldType == "string":
            s = v
        return s
    
    def toString(self):
        return "({}, {}, {})".format(self.fieldName, self.fieldType, "Index" if self.index != 0 else "NoIndex")
    
    def calExp(self, exp):
        v = None
        res = FieldCalRes()
        if exp.compareOp == "<":
            res.left = 0
            v = self.string2Value(exp.value)
            res.right = self.value2Uid(v)
            if res.right > 0:
                res.right -= 1
        elif exp.compareOp == "=":
            v = self.string2Value(exp.value)
            res.left = self.value2Uid(v)
            res.right = res.left
        elif exp.compareOp == ">":
            res.right = 18446744073709551615  # Long.MAX_VALUE
            v = self.string2Value(exp.value)
            res.left = self.value2Uid(v) + 1
        return res

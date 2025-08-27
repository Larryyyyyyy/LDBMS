import struct
import backend.tbm.Field
import backend.tm.TransactionManager
'''
Table 维护了表结构
二进制结构如下：
[TableName][NextTable]
[Field1Uid][Field2Uid]...[FieldNUid]
'''
def loadTable(tbm, uid):
    raw = None
    try:
        raw = tbm.vm.read(backend.tm.TransactionManager.SUPER_XID, uid)
    except Exception as e:
        raise e
    assert raw is not None
    return Table(tbm, uid).parseSelf(raw)

def createTable(tbm, nextUid, xid, create):
    tb = Table(tbm, 0, create.tableName, b'0', nextUid)
    for i in range(len(create.fieldName)):
        fieldName = create.fieldName[i]
        fieldType = create.fieldType[i]
        indexed = False
        for j in range(len(create.index)):
            if fieldName == create.index[j]:
                indexed = True
                break
        tb.fields.append(backend.tbm.Field.createField(tb, xid, fieldName, fieldType, indexed))
    return tb.persistSelf(xid)

class Table(object):
    def __init__(self, tbm, uid = 0, name = "", status = b'0', nextUid = 0):
        self.tbm = tbm
        self.uid = uid
        self.name = name
        self.status = status
        self.nextUid = nextUid
        self.fields = []

    class CalWhereRes(object):
        def __init__(self, l0 = 0, r0 = 0, l1 = 0, r1 = 0, single = False):
            self.l0 = l0
            self.r0 = r0
            self.l1 = l1
            self.r1 = r1
            self.single = single

    def parseSelf(self, raw):
        length = struct.unpack('>i', raw[0 : 4])[0]
        self.name = raw[4 : 4 + length].decode('utf-8')
        position = length + 4
        self.nextUid = struct.unpack('>q', raw[position : position + 8])[0]
        position += 8
        while position < len(raw):
            uid = struct.unpack('>q', raw[position : position + 8])[0]
            position += 8
            self.fields.append(backend.tbm.Field.loadField(self, uid))
        return self
    
    def persistSelf(self, xid):
        nameRaw = struct.pack('>i', len(self.name)) + self.name.encode('utf-8')
        nextRaw = struct.pack('>q', self.nextUid)
        fieldRaw = b''
        for field in self.fields:
            fieldRaw += struct.pack('>q', field.uid)
        self.uid = self.tbm.vm.insert(xid, nameRaw + nextRaw + fieldRaw)
        return self
    
    def delete(self, xid, delete):
        uids = self.parseWhere(delete.where)
        count = 0
        for uid in uids:
            if self.tbm.vm.delete(xid, uid):
                count += 1
        return count
    
    def update(self, xid, update):
        uids = self.parseWhere(update.where)
        fd = None
        for f in self.fields:
            if f.fieldName == update.fieldName:
                fd = f
                break
        if fd is None:
            raise Exception("FieldNotFoundException")
        value = fd.string2Value(update.value)
        count = 0
        for uid in uids:
            raw = self.tbm.vm.read(xid, uid)
            if raw is None:
                continue
            self.tbm.vm.delete(xid, uid)
            entry = self.parseEntry(raw)
            entry[fd.fieldName] = value
            raw = self.entry2Raw(entry)
            newUid = self.tbm.vm.insert(xid, raw)
            count += 1
            for field in self.fields:
                if field.isIndexed():
                    field.insert(entry.get(field.fieldName), newUid)
        return count
    
    def read(self, xid, read):
        uids = self.parseWhere(read.where)
        sb = []
        for uid in uids:
            raw = self.tbm.vm.read(xid, uid)
            if raw is None:
                continue
            entry = self.parseEntry(raw)
            sb.append(self.printEntry(entry) + "\n")
        return ''.join(sb)
    
    def insert(self, xid, insert):
        entry = self.string2Entry(insert.values)
        raw = self.entry2Raw(entry)
        uid = self.tbm.vm.insert(xid, raw)
        for field in self.fields:
            if field.isIndexed():
                field.insert(entry.get(field.fieldName), uid)

    def string2Entry(self, values):
        if len(values) != len(self.fields):
            raise Exception("InvalidValuesException")
        entry = {}
        for i in range(len(self.fields)):
            field = self.fields[i]
            value = field.string2Value(values[i])
            entry[field.fieldName] = value
        return entry
    
    def parseWhere(self, where):
        l0 = 0
        r0 = 0
        l1 = 0
        r1 = 0
        single = False
        fd = None
        if where is None:
            for field in self.fields:
                if field.isIndexed():
                    fd = field
                    break
            l0 = 0
            r0 = 9223372036854775807
            single = True
        else:
            for field in self.fields:
                if field.fieldName == where.singleExp1.field:
                    if not field.isIndexed():
                        raise Exception("FieldNotIndexedException")
                    fd = field
                    break
            if fd is None:
                raise Exception("FieldNotIndexedException")
            res = self.calWhere(fd, where)
            l0 = res.l0
            r0 = res.r0
            l1 = res.l1
            r1 = res.r1
            single = res.single
        uids = fd.search(l0, r0)
        if single == False:
            tmp = fd.search(l1, r1)
            uids.extend(tmp)
        return uids
    
    def calWhere(self, fd, where):
        res = self.CalWhereRes()
        if where.logicOp == "":
            res.single = True
            fieldCalRes = fd.calExp(where.singleExp1)
            res.l0 = fieldCalRes.left
            res.r0 = fieldCalRes.right
        elif where.logicOp == "or":
            res.single = False
            fieldCalRes = fd.calExp(where.singleExp1)
            res.l0 = fieldCalRes.left
            res.r0 = fieldCalRes.right
            fieldCalRes = fd.calExp(where.singleExp2)
            res.l1 = fieldCalRes.left
            res.r1 = fieldCalRes.right
        elif where.logicOp == "and":
            res.single = True
            fieldCalRes = fd.calExp(where.singleExp1)
            res.l0 = fieldCalRes.left
            res.r0 = fieldCalRes.right
            fieldCalRes = fd.calExp(where.singleExp2)
            res.l1 = fieldCalRes.left
            res.r1 = fieldCalRes.right
            if res.l1 > res.l0:
                res.l0 = res.l1
            if res.r1 < res.r0:
                res.r0 = res.r1
        else:
            raise Exception("InvalidLogOpException")
        return res
    
    def printEntry(self, entry):
        sb = "["
        for i in range(len(self.fields)):
            field = self.fields[i]
            sb += field.printValue(entry.get(field.fieldName))
            if i == len(self.fields) - 1:
                sb += "]"
            else:
                sb += ", "
        return sb
    
    def parseEntry(self, raw):
        pos = 0
        entry = {}
        for field in self.fields:
            r = field.parseValue(raw[pos:])
            entry[field.fieldName] = r.v
            pos += r.shift
        return entry
    
    def entry2Raw(self, entry):
        raw = b''
        for field in self.fields:
            raw += field.value2Raw(entry.get(field.fieldName))
        return raw
    
    def toString(self):
        sb = "{"
        sb += self.name + ": "
        for field in self.fields:
            sb += field.toString()
            if field == self.fields[-1]:
                sb += "}"
            else:
                sb += ", "
        return sb
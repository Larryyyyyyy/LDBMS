import struct
import threading
import backend.tbm.Booter
import backend.tbm.Table
import backend.parser.statement.Statements as Statements
from backend.vm.VersionManager import VersionManager
from backend.dm.DataManager import DataManager
from backend.tbm.Booter import Booter
class BeginRes(object):
    def __init__(self, xid = 0, result = b''):
        self.xid = xid
        self.result = result

class TableManager:
    def __init__(self, vm: VersionManager, dm: DataManager, booter: Booter):
        self.vm = vm
        self.dm = dm
        self.booter = booter
        self.tableCache = {}
        self.xidTableCache = {}
        self.lock = threading.RLock()
        self.loadTables()
    
    def loadTables(self) -> None:
        uid = self.firstTableUid()
        while uid:
            tb = backend.tbm.Table.loadTable(self, uid)
            uid = tb.nextUid
            self.tableCache[tb.name] = tb

    def firstTableUid(self) -> int:
        raw = self.booter.load()
        return struct.unpack('>q', raw)[0]
    
    def updateFirstTableUid(self, uid: int) ->None:
        raw = struct.pack('>q', uid)
        self.booter.update(raw)

    def begin(self, begin: Statements.Begin) -> BeginRes:
        res = BeginRes()
        level = 1 if begin.isRepeatableRead else 0
        res.xid = self.vm.begin(level)
        res.result = b'begin'
        return res
    
    def commit(self, xid: int) -> bytes:
        self.vm.commit(xid)
        return b'commit'
    
    def abort(self, xid: int) -> bytes:
        self.vm.abort(xid)
        return b'abort'
    
    def show(self, xid: int) -> bytes:
        self.lock.acquire()
        try:
            sb = []
            for tb in self.tableCache.values():
                sb.append(tb.toString())
                sb.append('\n')
            t = self.xidTableCache.get(xid)
            if t is None:
                return b'\n'
            for tb in t:
                sb.append(tb.toString())
                sb.append('\n')
            return ''.join(sb).encode('utf-8')
        finally:
            self.lock.release()

    def create(self, xid: int, create: Statements.Create) -> bytes:
        self.lock.acquire()
        try:
            if self.tableCache.get(create.tableName):
                raise Exception("DuplicatedTableException")
            table = backend.tbm.Table.createTable(self, self.firstTableUid(), xid, create)
            self.updateFirstTableUid(table.uid)
            self.tableCache[create.tableName] = table
            if xid not in self.xidTableCache:
                self.xidTableCache[xid] = []
            self.xidTableCache[xid].append(table)
            return ("create " + create.tableName).encode('utf-8')
        finally:
            self.lock.release()

    def insert(self, xid: int, insert: Statements.Insert) -> bytes:
        self.lock.acquire()
        table = self.tableCache.get(insert.tableName)
        self.lock.release()
        if table is None:
            raise Exception("TableNotFoundException")
        table.insert(xid, insert)
        return b'insert'
    
    def read(self, xid: int, read: Statements.Select) -> bytes:
        self.lock.acquire()
        table = self.tableCache.get(read.tableName)
        self.lock.release()
        if table is None:
            raise Exception("TableNotFoundException")
        return table.read(xid, read).encode('utf-8')
    
    def update(self, xid: int, update: Statements.Update) -> bytes:
        self.lock.acquire()
        table = self.tableCache.get(update.tableName)
        self.lock.release()
        if table is None:
            raise Exception("TableNotFoundException")
        count = table.update(xid, update)
        return ("update " + str(count)).encode('utf-8')
    
    def delete(self, xid: int, delete: Statements.Delete) -> bytes:
        self.lock.acquire()
        table = self.tableCache.get(delete.tableName)
        self.lock.release()
        if table is None:
            raise Exception("TableNotFoundException")
        count = table.delete(xid, delete)
        return ("delete " + str(count)).encode('utf-8')

def create(path: str, vm: VersionManager, dm: DataManager) -> TableManager:
    booter = backend.tbm.Booter.create(path)
    booter.update(struct.pack('>q', 0))
    return TableManager(vm, dm, booter)

def fileopen(path: str, vm: VersionManager, dm: DataManager) -> TableManager:
    booter = backend.tbm.Booter.fileopen(path)
    return TableManager(vm, dm, booter)

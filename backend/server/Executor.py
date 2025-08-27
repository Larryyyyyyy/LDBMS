import backend.parser.statement.Statements as Statements
import backend.parser.Parser as Parser

class Executor(object):
    def __init__(self, tbm):
        self.tbm = tbm
        self.xid = 0

    def close(self):
        if self.xid:
            print(f"Abnormal Abort: {self.xid}")
            self.tbm.abort(self.xid)

    def execute(self, sql):
        print(f"Execute: {sql.decode('utf-8')}")
        stat = Parser.Parse(sql)
        if isinstance(stat, Statements.Begin):
            if self.xid != 0:
                raise Exception("NestedTransactionException")
            res = self.tbm.begin(stat)
            self.xid = res.xid
            return res.result
        elif isinstance(stat, Statements.Commit):
            if self.xid == 0:
                raise Exception("NoTransactionException")
            res = self.tbm.commit(self.xid)
            self.xid = 0
            return res
        elif isinstance(stat, Statements.Abort):
            if self.xid == 0:
                raise Exception("NoTransactionException")
            res = self.tbm.abort(self.xid)
            self.xid = 0
            return res
        else:
            return self.execute2(stat)
    
    def execute2(self, stat):
        tmpTransaction = False
        e = None
        if self.xid == 0:
            tmpTransaction = True
            res = self.tbm.begin(Statements.Begin())
            self.xid = res.xid
        try:
            if isinstance(stat, Statements.Show):
                return self.tbm.show(self.xid)
            elif isinstance(stat, Statements.Create):
                return self.tbm.create(self.xid, stat)
            elif isinstance(stat, Statements.Select):
                return self.tbm.read(self.xid, stat)
            elif isinstance(stat, Statements.Insert):
                return self.tbm.insert(self.xid, stat)
            elif isinstance(stat, Statements.Delete):
                return self.tbm.delete(self.xid, stat)
            elif isinstance(stat, Statements.Update):
                return self.tbm.update(self.xid, stat)
        except Exception as e1:
            e = e1
            raise e
        finally:
            if tmpTransaction:
                if e is not None:
                    self.tbm.abort(self.xid)
                else:
                    self.tbm.commit(self.xid)
                self.xid = 0

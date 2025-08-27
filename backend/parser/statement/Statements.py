class Abort(object):
    def __init__(self):
        pass

class Begin(object):
    def __init__(self, isRepeatableRead = False):
        self.isRepeatableRead = isRepeatableRead

class Commit(object):
    def __init__(self):
        pass

class Create(object):
    def __init__(self, tableName = "", fieldName = [], fieldType = [], index = []):
        self.tableName = tableName
        self.fieldName = fieldName
        self.fieldType = fieldType
        self.index = index

class Delete(object):
    def __init__(self, tableName = "", where = None):
        self.tableName = tableName
        self.where = where

class Drop(object):
    def __init__(self, tableName = ""):
        self.tableName = tableName

class Insert(object):
    def __init__(self, tableName = "", values = []):
        self.tableName = tableName
        self.values = values

class Select(object):
    def __init__(self, tableName = "", fields = [], where = None):
        self.tableName = tableName
        self.fields = fields
        self.where = where

class Show(object):
    def __init__(self):
        pass

class SingleExpression(object):
    def __init__(self, field = "", compareOp = "", value = ""):
        self.field = field
        self.compareOp = compareOp
        self.value = value

class Update(object):
    def __init__(self, tableName = "", fieldName = "", value = "", where = None):
        self.tableName = tableName
        self.fieldName = fieldName
        self.value = value
        self.where = where

class Where(object):
    def __init__(self, singleExp1 = None, logicOp = "", singleExp2 = None):
        self.singleExp1 = singleExp1
        self.logicOp = logicOp
        self.singleExp2 = singleExp2

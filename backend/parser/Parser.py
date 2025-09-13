'''
该数据库支持的SQL语法如下
<begin statement>
    begin [isolation level (read committedrepeatable read)]
        begin isolation level read committed

<commit statement>
    commit

<abort statement>
    abort

<create statement>
    create table <table name>
    <field name> <field type>
    <field name> <field type>
    ...
    <field name> <field type>
    [(index <field name list>)]
    example:
        create table students
        id int32,
        name string,
        age int32
        (index id name)

<drop statement>
    drop table <table name>
    example:
        drop table students

<select statement>
    select (*<field name list>) from <table name> [<where statement>]
    example:
        select * from student where id = 1
        select name from student where id > 1 and id < 4
        select name, age, id from student where id = 12

<insert statement>
    insert into <table name> values <value list>
    example:
        insert into student values 5 "Zhang Yuanjia" 22

<delete statement>
    delete from <table name> <where statement>
    example:
        delete from student where name = "Zhang Yuanjia"

<update statement>
    update <table name> set <field name>=<value> [<where statement>]
    example:
        update student set name = "ZYJ" where id = 5

<where statement>
    where <field name> (><=) <value> [(andor) <field name> (><=) <value>]
    example:
        where age > 10 or age < 3

<field name> <table name>
    [a-zA-Z][a-zA-Z0-9_]*

<field type>
    int32 int64 string

<value>
    .*
'''
import backend.parser.statement.Statements
from backend.parser.Tokenizer import Tokenizer

def Parse(statement: bytearray | bytes):
    tokenizer = Tokenizer(statement)
    token = tokenizer.peek()
    tokenizer.pop()
    stat = None
    statErr = None
    try:
        if token == "begin":
            stat = parseBegin(tokenizer)
        elif token == "commit":
            stat = parseCommit(tokenizer)
        elif token == "abort":
            stat = parseAbort(tokenizer)
        elif token == "create":
            stat = parseCreate(tokenizer)
        elif token == "drop":
            stat = parseDrop(tokenizer)
        elif token == "select":
            stat = parseSelect(tokenizer)
        elif token == "insert":
            stat = parseInsert(tokenizer)
        elif token == "delete":
            stat = parseDelete(tokenizer)
        elif token == "update":
            stat = parseUpdate(tokenizer)
        elif token == "show":
            stat = parseShow(tokenizer)
        else:
            raise Exception("InvalidCommandException")
    except Exception as e:
        statErr = e
    try:
        next = tokenizer.peek()
        if next != "":
            errStat = tokenizer.errStat()
            statErr = RuntimeError("Invalid statement: " + errStat.decode('utf-8'))
    except Exception as e:
        errStat = tokenizer.errStat()
        statErr = RuntimeError("Invalid statement: " + errStat.decode('utf-8'))
    if statErr is not None:
        raise statErr
    return stat

def parseShow(tokenizer: Tokenizer):
    tmp = tokenizer.peek()
    if tmp == "":
        return backend.parser.statement.Statements.Show()
    raise Exception("InvalidCommandException")

def parseUpdate(tokenizer: Tokenizer):
    update = backend.parser.statement.Statements.Update()
    update.tableName = tokenizer.peek()
    tokenizer.pop()
    if tokenizer.peek() != "set":
        raise Exception("InvalidCommandException")
    tokenizer.pop()
    update.fieldName = tokenizer.peek()
    tokenizer.pop()
    if tokenizer.peek() != "=":
        raise Exception("InvalidCommandException")
    tokenizer.pop()
    update.value = tokenizer.peek()
    tokenizer.pop()
    tmp = tokenizer.peek()
    if tmp == "":
        update.where = None
        return update

    update.where = parseWhere(tokenizer)
    return update

def parseDelete(tokenizer: Tokenizer):
    delete = backend.parser.statement.Statements.Delete()
    if tokenizer.peek() != "from":
        raise Exception("InvalidCommandException")
    tokenizer.pop()
    delete.tableName = tokenizer.peek()
    if not isName(delete.tableName):
        raise Exception("InvalidCommandException")
    tokenizer.pop()
    delete.where = parseWhere(tokenizer)
    return delete

def parseInsert(tokenizer: Tokenizer):
    insert = backend.parser.statement.Statements.Insert()
    if tokenizer.peek() != "into":
        raise Exception("InvalidCommandException")
    tokenizer.pop()
    insert.tableName = tokenizer.peek()
    if not isName(insert.tableName):
        raise Exception("InvalidCommandException")
    tokenizer.pop()
    if tokenizer.peek() != "values":
        raise Exception("InvalidCommandException")
    values = []
    while True:
        tokenizer.pop()
        value = tokenizer.peek()
        if value == "":
            break
        values.append(value)
    insert.values = values
    return insert

def parseSelect(tokenizer: Tokenizer):
    select = backend.parser.statement.Statements.Select()
    if tokenizer.peek() == "*":
        select.fields.append("*")
        tokenizer.pop()
    else:
        while True:
            field = tokenizer.peek()
            if not isName(field):
                raise Exception("InvalidCommandException")
            select.fields.append(field)
            tokenizer.pop()
            if tokenizer.peek() == ",":
                tokenizer.pop()
            else:
                break
    if tokenizer.peek() != "from":
        raise Exception("InvalidCommandException")
    tokenizer.pop()
    select.tableName = tokenizer.peek()
    if not isName(select.tableName):
        raise Exception("InvalidCommandException")
    tokenizer.pop()
    tmp = tokenizer.peek()
    if tmp == "":
        select.where = None
        return select
    select.where = parseWhere(tokenizer)
    return select

def parseWhere(tokenizer: Tokenizer):
    where = backend.parser.statement.Statements.Where()
    if tokenizer.peek() != "where":
        raise Exception("InvalidCommandException")
    tokenizer.pop()
    singleExp1 = parseSingleExp(tokenizer)
    where.singleExp1 = singleExp1
    logicOp = tokenizer.peek()
    if logicOp == "":
        where.logicOp = ""
        return where
    if not isLogicOp(logicOp):
        raise Exception("InvalidCommandException")
    where.logicOp = logicOp
    tokenizer.pop()

    singleExp2 = parseSingleExp(tokenizer)
    where.singleExp2 = singleExp2
    if tokenizer.peek() != "":
        raise Exception("InvalidCommandException")
    return where

def parseSingleExp(tokenizer: Tokenizer):
    singleExp = backend.parser.statement.Statements.SingleExpression()
    field = tokenizer.peek()
    if not isName(field):
        raise Exception("InvalidCommandException")
    singleExp.field = field
    tokenizer.pop()
    compareOp = tokenizer.peek()
    if not isCmpOp(compareOp):
        raise Exception("InvalidCommandException")
    singleExp.compareOp = compareOp
    tokenizer.pop()
    singleExp.value = tokenizer.peek()
    tokenizer.pop()
    return singleExp

def parseDrop(tokenizer: Tokenizer):
    if tokenizer.peek() != "table":
        raise Exception("InvalidCommandException")
    tokenizer.pop()
    tableName = tokenizer.peek()
    if not isName(tableName):
        raise Exception("InvalidCommandException")
    tokenizer.pop()
    if tokenizer.peek() != "":
        raise Exception("InvalidCommandException")
    drop = backend.parser.statement.Statements.Drop(tableName)
    return drop

def parseCreate(tokenizer: Tokenizer):
    if tokenizer.peek() != "table":
        raise Exception("InvalidCommandException")
    tokenizer.pop()
    create = backend.parser.statement.Statements.Create()
    tableName = tokenizer.peek()
    if not isName(tableName):
        raise Exception("InvalidCommandException")
    create.tableName = tableName
    fieldNames = []
    fieldTypes = []
    while True:
        tokenizer.pop()
        fieldName = tokenizer.peek()
        if fieldName == "(":
            break
        if not isName(fieldName):
            raise Exception("InvalidCommandException")
        fieldNames.append(fieldName)
        tokenizer.pop()
        fieldType = tokenizer.peek()
        if not isType(fieldType):
            raise Exception("InvalidCommandException")
        fieldTypes.append(fieldType)
        tokenizer.pop()
        nextToken = tokenizer.peek()
        if nextToken == ",":
            continue
        elif nextToken == "":
            raise Exception("InvalidCommandException")
        elif nextToken == "(":
            break
        else:
            raise Exception("InvalidCommandException")
    create.fieldName = fieldNames
    create.fieldType = fieldTypes
    tokenizer.pop()
    if tokenizer.peek() != "index":
        raise Exception("InvalidCommandException")
    indexes = []
    while True:
        tokenizer.pop()
        indexField = tokenizer.peek()
        if indexField == ")":
            break
        if not isName(indexField):
            raise Exception("InvalidCommandException")
        indexes.append(indexField)
    create.index = indexes
    tokenizer.pop()
    if tokenizer.peek() != "":
        raise Exception("InvalidCommandException")
    return create

def parseAbort(tokenizer: Tokenizer):
    if tokenizer.peek() != "":
        raise Exception("InvalidCommandException")
    return backend.parser.statement.Statements.Abort()

def parseCommit(tokenizer: Tokenizer):
    if tokenizer.peek() != "":
        raise Exception("InvalidCommandException")
    return backend.parser.statement.Statements.Commit()

def parseBegin(tokenizer: Tokenizer):
    if tokenizer.peek() != "isolation":
        return backend.parser.statement.Statements.Begin()
    tokenizer.pop()
    if tokenizer.peek() != "level":
        raise Exception("InvalidCommandException")
    tokenizer.pop()
    level = tokenizer.peek()
    if level == "read":
        tokenizer.pop()
        if tokenizer.peek() != "committed":
            raise Exception("InvalidCommandException")
        tokenizer.pop()
        if tokenizer.peek() != "":
            raise Exception("InvalidCommandException")
        return backend.parser.statement.Statements.Begin()
    elif level == "repeatable":
        tokenizer.pop()
        if tokenizer.peek() != "read":
            raise Exception("InvalidCommandException")
        tokenizer.pop()
        if tokenizer.peek() != "":
            raise Exception("InvalidCommandException")
        return backend.parser.statement.Statements.Begin(isRepeatableRead = True)
    else:
        raise Exception("InvalidCommandException")

def isName(name: str) -> bool:
    return not (len(name) == 1 and not Tokenizer.isAlphaBeta(name.encode('utf-8')[0]))

def isType(tp: str) -> bool:
    return tp in ("int32", "int64", "string")

def isCmpOp(op: str) -> bool:
    return op in ("=", ">", "<")

def isLogicOp(op: str) -> bool:
    return op in ("and", "or")
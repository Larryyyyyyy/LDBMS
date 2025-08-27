import backend.tm.TransactionManager as TransactionManager
import backend.dm.DataManager as DataManager
from backend.vm.VersionManager import VersionManager
import backend.tbm.TableManager as TableManager
from backend.server.Server import Server
from argparse import ArgumentParser

port = 9999
DEFALUT_MEM = (1<<20)*64
KB = 1 << 10
MB = 1 << 20
GB = 1 << 30

def createDB(path):
    tm = TransactionManager.create(path)
    dm = DataManager.create(path, DEFALUT_MEM, tm)
    vm = VersionManager(tm, dm)
    TableManager.create(path, vm, dm)
    #tm.close()
    dm.close()

def openDB(path, mem = DEFALUT_MEM):
    tm = TransactionManager.fileopen(path)
    dm = DataManager.fileopen(path, mem, tm)
    vm = VersionManager(tm, dm)
    tbm = TableManager.fileopen(path, vm, dm)
    server = Server(port, tbm)
    server.start()

def parseMem(memStr):
    if memStr is None or memStr == "":
        return DEFALUT_MEM
    if len(memStr) < 2:
        raise ValueError("Invalid memory size format")
    unit = memStr[-2:]
    memNum = int(memStr[:-2])
    if unit == "KB":
        return memNum * KB
    elif unit == "MB":
        return memNum * MB
    elif unit == "GB":
        return memNum * GB
    else:
        raise ValueError("Invalid memory size unit")

'''
creat a database:
python Launcher_server.py -create "C:/Users/91026/Desktop/vscode/py/LDBMS/tmp/ldbms"
start the database:
python Launcher_server.py -open "C:/Users/91026/Desktop/vscode/py/LDBMS/tmp/ldbms"
'''

if __name__ == "__main__":
    parser = ArgumentParser(description="Database Launcher")
    parser.add_argument("-open", type=str, help="Open existing database at DBPath")
    parser.add_argument("-create", type=str, help="Create new database at DBPath")
    parser.add_argument("-mem", type=str, default="64MB", help="Memory size (default: 64MB)")
    args = parser.parse_args()
    if args.open:
        openDB(args.open, parseMem(args.mem))
    elif args.create:
        createDB(args.create)
    else:
        print("Usage: launcher -open DBPath | -create DBPath [-mem MemorySize]")

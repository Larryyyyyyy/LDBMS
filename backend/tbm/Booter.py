# 记录第一个表的uid
import os

BOOTER_SUFFIX = ".bt"
BOOTER_TMP_SUFFIX = ".bt_tmp"

class Booter:
    """
    管理启动文件操作
    包括创建、打开、加载和更新启动文件
    """
    def __init__(self, path: str):
        self.path = path
        self.file = None

    def load(self) -> bytes:
        with open(self.path + BOOTER_SUFFIX, 'rb') as f:
            return f.read()

    def update(self, data: bytearray | bytes) -> None:
        tmp_path = self.path + BOOTER_TMP_SUFFIX
        with open(tmp_path, 'wb') as tmp_file:
            tmp_file.write(data)
        
        os.replace(tmp_path, self.path + BOOTER_SUFFIX)

def create(path: str) -> Booter:
    remove_bad_tmp(path)
    f = open(path + BOOTER_SUFFIX, 'w+b')
    if not f:
        raise Exception("File already exists")
    return Booter(path)

def fileopen(path: str) -> Booter:
    remove_bad_tmp(path)
    f = open(path + BOOTER_SUFFIX, 'r+b')
    if not f:
        raise Exception("File does not exist")
    return Booter(path)

def remove_bad_tmp(path: str) -> None:
    try:
        os.remove(path + BOOTER_TMP_SUFFIX)
    except OSError:
        pass

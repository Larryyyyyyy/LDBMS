# 记录第一个表的uid
import os

BOOTER_SUFFIX = ".bt"
BOOTER_TMP_SUFFIX = ".bt_tmp"

def create(path):
    remove_bad_tmp(path)
    f = open(path + BOOTER_SUFFIX, 'w+b')
    if not f:
        raise Exception("File already exists")
    return Booter(path)

def fileopen(path):
    remove_bad_tmp(path)
    f = open(path + BOOTER_SUFFIX, 'r+b')
    if not f:
        raise Exception("File does not exist")
    return Booter(path)

def remove_bad_tmp(path):
    try:
        os.remove(path + BOOTER_TMP_SUFFIX)
    except OSError:
        pass

class Booter:
    """
    管理启动文件操作
    包括创建、打开、加载和更新启动文件
    """
    def __init__(self, path):
        self.path = path
        self.file = None

    def load(self):
        with open(self.path + BOOTER_SUFFIX, 'rb') as f:
            return f.read()

    def update(self, data):
        tmp_path = self.path + BOOTER_TMP_SUFFIX
        with open(tmp_path, 'wb') as tmp_file:
            tmp_file.write(data)
        
        os.replace(tmp_path, self.path + BOOTER_SUFFIX)
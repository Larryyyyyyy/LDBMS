from abc import ABC, abstractmethod
import traceback
import time
import threading

class AbstractClass(ABC):
    def __init__(self, maxResources):
        self.maxResources = maxResources
        self.count = 0                             # 获取资源的操作个数
        self.cache = {}                            # 实际缓存的数据
        self.references = {}                       # 资源的引用个数
        self.getting = {}                          # 正在被获取的资源
        self.lock = threading.RLock()

    def get(self, key):                            # 获取资源,检查目的资源是否存在读取冲突等情况
        while 1:
            self.lock.acquire()
            if key in self.getting:                # 请求的资源正在被其它进程获取
                self.lock.release()
                try:
                    time.sleep(0.001)
                except Exception as e:
                    traceback.print_exc()
                    continue
                continue
            if key in self.cache:                  # 资源在缓存中
                obj = self.cache.get(key)
                self.references[key] += 1
                self.lock.release()
                return obj
            if self.maxResources > 0 and self.count == self.maxResources:
                self.lock.release()
                raise Exception("CacheFullException")               # 获取操作大于资源数
            self.count += 1
            self.getting[key] = True
            self.lock.release()
            break
        obj = None
        try:
            obj = self.getForCache(key)
        except Exception as e:
            self.lock.acquire()
            try:
                self.count -= 1
                self.getting.pop(key, None)
            finally:
                self.lock.release()
            raise e
        self.lock.acquire()
        try:
            self.getting.pop(key, None)
            self.cache[key] = obj
            self.references[key] = 1
        finally:
            self.lock.release()
        return obj
    
    def release(self, key):                        # 释放资源
        self.lock.acquire()
        try:
            ref = self.references[key] - 1
            if ref == 0:                           # 资源没有引用了,清除掉
                obj = self.cache[key]
                self.releaseForCache(obj)
                self.references.pop(key, None)
                self.cache.pop(key, None)
                self.count -= 1
            else:
                self.references[key] = ref
        finally:
            self.lock.release()
    
    def close(self):
        self.lock.acquire()
        try:
            for key in self.cache:
                obj = self.cache[key]
                self.releaseForCache(obj)
                self.references.pop(key, None)
                self.cache.pop(key, None)
        finally:
            self.lock.release()

    @abstractmethod
    def getForCache(key):
        pass

    @abstractmethod
    def releaseForCache(obj):
        pass
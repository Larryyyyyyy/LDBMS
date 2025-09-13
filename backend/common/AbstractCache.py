from abc import ABC, abstractmethod
import traceback
import time
import threading

class AbstractClass(ABC):
    def __init__(self, maxResources: int):
        self.maxResources = maxResources
        # 获取资源的操作个数
        self.count = 0
        # 实际缓存的数据
        self.cache = {}
        # 资源的引用个数
        self.references = {}
        # 资源正在被获取
        self.getting = {}
        self.lock = threading.RLock()

    def get(self, key: int) -> any:
        '''
        获取资源,检查目的资源是否存在读取冲突等情况
        '''
        while 1:
            self.lock.acquire()
            # 请求的资源正在被其它进程获取
            if key in self.getting:
                self.lock.release()
                try:
                    time.sleep(0.001)
                except Exception as e:
                    traceback.print_exc()
                    continue
                continue
            # 资源在缓存中
            if key in self.cache:
                obj = self.cache.get(key)
                self.references[key] += 1
                self.lock.release()
                return obj
            # 获取操作大于资源数
            if self.maxResources > 0 and self.count == self.maxResources:
                self.lock.release()
                raise Exception("CacheFullException")
            # 不在缓存中的资源, 从数据源中读取
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
    
    def release(self, key: int) -> None:
        '''
        释放资源
        '''
        self.lock.acquire()
        try:
            ref = self.references[key] - 1
            # 资源没有引用了, 清除掉
            if ref == 0:
                obj = self.cache[key]
                self.releaseForCache(obj)
                self.references.pop(key, None)
                self.cache.pop(key, None)
                self.count -= 1
            else:
                self.references[key] = ref
        finally:
            self.lock.release()
    
    def close(self) -> None:
        '''
        释放缓存中的所有数据
        '''
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
# 缓存每一页的空闲空间
# 将一页的空间划分成了 40 个区间
# 在启动时, 遍历所有的页面信息, 获取页面的空闲空间, 安排到这 40 个区间中
# insert 在请求一个页时, 会首先将所需的空间向上取整, 映射到某一个区间, 随后取出这个区间的任何一页, 都可以满足需求
import threading
from backend.dm.pageCache import PageCache
from backend.dm.pageIndex.PageInfo import PageInfo

# 区间数
INTERVALS_NO = 40
# 每一个区间所需的空间
THRESHOLD = PageCache.PAGE_SIZE // INTERVALS_NO

class PageIndex(object):
    def __init__(self):
        self.lock = threading.RLock()
        self.lists = [[] for _ in range(INTERVALS_NO + 1)]

    def add(self, pgno: int, freeSpace: int) -> None:
        '''
        向 PageIndex 添加页面信息
        '''
        self.lock.acquire()
        try:
            number = freeSpace // THRESHOLD
            self.lists[number].append(PageInfo(pgno, freeSpace))
        finally:
            self.lock.release()

    def select(self, spaceSize: int) -> PageInfo | None:
        '''
        从 PageIndex 中选择一个合适的页面来存储需要 spaceSize 大小数据的操作
        '''
        self.lock.acquire()
        try:
            number = spaceSize // THRESHOLD
            if (number < INTERVALS_NO):
                number = number + 1
            while number <= INTERVALS_NO:
                # 查找具有更大可用空间的页面
                if (len(self.lists[number]) == 0):
                    number = number + 1
                    continue
                ret_val = self.lists[number].pop(0)
                return ret_val
            return None
        finally:
            self.lock.release()

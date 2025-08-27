'''
版本的可见性与事务的隔离度是相关的
最低的事务隔离程度是"读提交"(Read Committed),即事务在读取数据时,只能读取已经提交事务产生的数据。

实现读提交,为每个版本维护了两个变量,就是 XMIN 和 XMAX:

XMIN:创建该版本的事务编号
XMAX:删除该版本的事务编号
XMIN 应当在版本创建时填写,而 XMAX 则在版本被删除,或者有新版本出现时填写
'''
from backend.tm.TransactionManager import TransactionManager

'''
在撤销或是回滚事务很简单,只需要将这个事务标记为 aborted 即可
每个事务都只能看到其他 committed 的事务所产生的数据
一个 aborted 事务产生的数据就不会对其他事务产生任何影响了,也就相当于这个事务不曾存在过
'''
# 版本跳跃的检查:
# 取出要修改的数据 X 的最新提交版本,并检查该最新版本的创建者对当前事务是否可见:
def isVeresionSkip(tm, t, e):
    xmax = e.getXmax()
    if t.level == 0:
        return False
    else:
        return tm.isCommitted(xmax) and (xmax > t.xid or t.isInSnapshot(xmax))

def isVisible(tm, t, e):
    if t.level == 0:
        return readCommitted(tm, t, e)
    else:
        return repeatableRead(tm, t, e)

'''
由 Ti 创建且还未被删除
或
由一个已提交的事务创建且(尚未删除或由一个未提交的事务删除)
这样的事务对 Ti 可见
'''
def readCommitted(tm, t, e):
    xid = t.xid
    xmin = e.getXmin()
    xmax = e.getXmax()
    if xmin == xid and xmax == 0:
        return True
    if tm.isCommitted(xmin):
        if xmax == 0:
            return True
        if xmax != xid:
            if not tm.isCommitted(xmax):
                return True
    return False

'''
可重复读问题:
规定:事务只能读取它开始时,就已经结束的那些事务产生的数据版本
所以当前事务需要忽略:
在本事务后开始的事务的数据 和 本事务开始时还是 active 状态的事务的数据

由 Ti 创建且尚未被删除
或
由一个已提交的事务创建且这个事务小于 Ti 且这个事务在 Ti 开始前提交且
(尚未被删除或(由其他事务删除且(这个事务尚未提交或这个事务在 Ti 开始之后才开始或这个事务在 Ti 开始前还未提交)))
'''
def repeatableRead(tm, t, e):
    xid = t.xid
    xmin = e.getXmin()
    xmax = e.getXmax
    if xmin == xid and xmax == 0:
        return True
    if tm.isCommitted(xmin) and xmin < xid and t.isInSnapshot(xmin) == False:
        if xmax == 0:
            return True
        if xmax != xid:
            if tm.isCommitted(xmax) == False or xmax >xid or t.isInSnapshot(xmax):
                return True
    return False
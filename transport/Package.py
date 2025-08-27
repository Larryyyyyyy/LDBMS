'''
传输的最基本结构
'''
class Package(object):
    def __init__(self, data, err):
        self.data = data
        self.err = err
'''
每个 Package 在发送前,由 Encoder 编码为字节数组,在对方收到后同样会由 Encoder 解码成 Package 对象
编码和解码的规则如下:
[Flag][data]
若 flag 为 0,表示发送的是数据, data 即为这份数据本身
如果 flag 为 1,表示发送的是错误, data 是 Exception.getMessage() 的错误提示信息
'''
from transport.Package import Package
class Encoder(object):
    def encode(self, pkg: Package) -> bytes:
        '''
        编码
        '''
        if pkg.err is not None:
            err = pkg.err
            msg = "Intern server error!"
            if err.args and len(err.args) > 0:
                msg = err.args[0]
            return bytes([1]) + msg.encode('utf-8')
        else:
            return bytes([0]) + pkg.data

    def decode(self, data: bytearray | bytes) -> Package:
        '''
        解码
        '''
        if len(data) < 1:
            raise Exception("InvalidPkgDataException")
        if data[0] == 0:
            return Package(data[1:], None)
        elif data[0] == 1:
            return Package(None, RuntimeError(data[1:].decode('utf-8')))
        else:
            raise Exception("InvalidPkgDataException")

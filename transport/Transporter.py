'''
编码之后的信息会通过 Transporter 类写入输出流发送出去
为了避免特殊字符造成问题,这里会将数据转成十六进制字符串(Hex String)并为信息末尾加上换行符
'''
import socket

class Transporter(object):
    def __init__(self, socket: socket.socket):
        self.socket = socket
        self.reader = socket.makefile('r', encoding = 'utf-8')
        self.writer = socket.makefile('w', encoding = 'utf-8')

    def send(self, data: bytearray | bytes) -> None:
        raw = self.hexEncode(data)
        self.writer.write(raw)
        self.writer.flush()

    def receive(self) -> bytes:
        line = self.reader.readline()
        if not line:
            raise Exception("Connection closed")
        return self.hexDecode(line.strip())

    def close(self) -> None:
        self.writer.close()
        self.reader.close()
        self.socket.close()

    def hexEncode(self, buf: bytearray | bytes) -> str:
        return buf.hex() + '\n'

    def hexDecode(self, buf: bytearray | bytes) -> bytes:
        return bytes.fromhex(buf)

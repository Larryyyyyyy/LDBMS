from transport.Packager import Packager
from transport.Transporter import Transporter
from transport.Encoder import Encoder
from client.Client import Client
from client.Shell import Shell
import socket

class Launcher(object):
    def main(self):
        '''
        连接数据库服务器
        默认ip: 127.0.0.1
        默认端口: 9999
        '''
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", 9999))
        encoder = Encoder()
        transporter = Transporter(sock)
        packager = Packager(transporter, encoder)
        client = Client(packager)
        shell = Shell(client)
        shell.run()

'''
python Launcher_client.py
示例指令:
create table students id int32, name string, age int32 (index id name)
insert into students values 202430206 Larry 19
select * from students where id=202430206
begin
insert into students values 202430211 Eric 19
commit
select * from students where id>0
begin
delete from students where id=202430211
abort
'''

if __name__ == "__main__":
    launcher = Launcher()
    launcher.main()

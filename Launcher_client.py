from transport.Packager import Packager
from transport.Package import Package
from transport.Transporter import Transporter
from transport.Encoder import Encoder
from client.Client import Client
from client.Shell import Shell
import socket

class Launcher(object):
    def main(self):
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
create table test_table id int32, value int32 (index id)
insert into test_table values 10 33
select * from test_table where id=10
begin
insert into test_table values 20 34
commit
select * from test_table where id>0
begin
delete from test_table where id=10
abort
'''

if __name__ == "__main__":
    launcher = Launcher()
    launcher.main()
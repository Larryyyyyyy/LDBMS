import socket
import threading
import traceback
from backend.server.Executor import Executor
from transport.Encoder import Encoder
from transport.Package import Package
from transport.Packager import Packager
from transport.Transporter import Transporter
from backend.tbm.TableManager import TableManager

class Server(object):
    def __init__(self, port: int, tbm: TableManager):
        self.port = port
        self.tbm = tbm
    
    def start(self) -> None:
        ss = None
        try:
            ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ss.bind(('localhost', self.port))
            ss.listen(5)
        except socket.error as e:
            print(f"Socket error: {e}")
            return
        print(f"Server listening on port: {self.port}")
        try:
            while True:
                client_socket, addr = ss.accept()
                print(f"Connection established with {addr}")
                worker = HandleSocket(client_socket, self.tbm)
                worker.start()
        except socket.error as e:
            print(f"Error accepting connection: {e}")
        finally:
            if ss:
                ss.close()
    
class HandleSocket(threading.Thread):
    def __init__(self, socket: socket.socket, tbm: TableManager):
        threading.Thread.__init__(self)
        self.socket = socket
        self.tbm = tbm
    
    def run(self) -> None:
        address = self.socket.getpeername()
        print(f"Connected to {address[0]}:{address[1]}")
        packager = None
        try:
            transporter = Transporter(self.socket)
            encoder = Encoder()
            packager = Packager(transporter, encoder)
        except Exception as e:
            print(f"Error initializing packager: {e}")
            try:
                self.socket.close()
            except Exception as e1:
                traceback.print_exc()
            return
        exe = Executor(self.tbm)
        while True:
            pkg = None
            try:
                pkg = packager.receive()
            except Exception as e:
                break
            sql = pkg.data
            result = None
            error = None
            try:
                result = exe.execute(sql)
            except Exception as e:
                error = e
                traceback.print_exc()
            pkg = Package(result, error)
            try:
                packager.send(pkg)
            except Exception as e:
                traceback.print_exc()
                break
        exe.close()
        try:
            packager.close()
        except Exception as e:
            traceback.print_exc()

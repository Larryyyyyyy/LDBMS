from transport.Encoder import Encoder
from transport.Transporter import Transporter
from transport.Package import Package
class Packager(object):
    def __init__(self, transporter: Transporter, encoder: Encoder):
        self.transporter = transporter
        self.encoder = encoder

    def send(self, pkg: Package) -> None:
        data = self.encoder.encode(pkg)
        self.transporter.send(data)

    def receive(self) -> Package:
        data = self.transporter.receive()
        return self.encoder.decode(data)

    def close(self) -> None:
        self.transporter.close()

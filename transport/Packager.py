class Packager(object):
    def __init__(self, transporter, encoder):
        self.transporter = transporter
        self.encoder = encoder

    def send(self, pkg):
        data = self.encoder.encode(pkg)
        self.transporter.send(data)

    def receive(self):
        data = self.transporter.receive()
        return self.encoder.decode(data)

    def close(self):
        self.transporter.close()

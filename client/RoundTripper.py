from transport.Package import Package
from transport.Packager import Packager

class RoundTripper(object):
    def __init__(self, packager):
        self.packager = packager
    
    def roundTrip(self, pkg):
        self.packager.send(pkg)
        return self.packager.receive()

    def close(self):
        self.packager.close()

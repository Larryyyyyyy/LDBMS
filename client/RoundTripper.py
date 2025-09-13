from transport.Packager import Packager
from transport.Package import Package
class RoundTripper(object):
    def __init__(self, packager: Packager):
        self.packager = packager
    
    def roundTrip(self, pkg: Package) -> Package:
        self.packager.send(pkg)
        return self.packager.receive()

    def close(self) -> None:
        self.packager.close()

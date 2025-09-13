from transport.Package import Package
from client.RoundTripper import RoundTripper

class Client(object):
    def __init__(self, packager):
        self.rt = RoundTripper(packager)

    def execute(self, stat: bytearray | bytes) -> bytearray | bytes:
        pkg = Package(stat, None)
        res_pkg = self.rt.roundTrip(pkg)
        if res_pkg.err is not None:
            raise res_pkg.err
        return res_pkg.data

    def close(self) -> None:
        try:
            self.rt.close()
        except Exception as e:
            print(f"Error closing client: {e}")

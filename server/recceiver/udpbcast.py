import socket

from twisted.application import internet
from twisted.internet import udp

__all__ = ["SharedUDP", "SharedUDPServer"]


class SharedUDP(udp.Port):
    """A UDP socket which can share
    a port with other similarly configured
    sockets.  Broadcasts to this port will
    be copied to all sockets.
    However, unicast traffic will only be
    delivered to one (implementation defined)
    socket.
    """

    def createInternetSocket(self) -> socket.socket:
        sock = udp.Port.createInternetSocket(self)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        return sock


class SharedUDPServer(internet.UDPServer):
    """A UDP server using SharedUDP"""

    def _getPort(self) -> SharedUDP:
        from twisted.internet import (  # noqa: PLC0415
            reactor,  # importing reactor does more than just import it, so we want to delay this until we need it
        )

        R = getattr(self, "reactor", reactor)
        port = SharedUDP(reactor=R, *self.args, **self.kwargs)  # noqa: B026
        port.startListening()
        return port

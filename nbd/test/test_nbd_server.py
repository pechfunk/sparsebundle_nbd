from twisted.trial import unittest

from nbd.nbd import NBDServerProtocol, StringBlockDevice

class DummyTransport(object):
    def __init__(self):
        self.writtens = [ ]
    def write(self, s):
        self.writtens.append(s)
    def reset(self):
        self.writtens = []
    def __str__(self):
        return ''.join(self.writtens)

class NBDServerTest(unittest.TestCase):
    def setUp(self):
        self.bd = bd = StringBlockDevice('ABCDEFGHIJKL')
        self.prot = prot = NBDServerProtocol( bd )
        self.dt = dt = DummyTransport()
        prot.transport = dt

    def test_welcome_handshake(self):
        self.prot.connectionMade()
        self.assertEquals( 
            'NBDMAGIC' \
            + '\x00\x00\x42\x02\x81\x86\x12\x53' \
            + '\0\0\0\0\0\0\0\x0c' \
            + '\0' * 124, str(self.dt))

    def test_valid_read_request(self):
        self.prot.connectionMade()
        self.dt.reset()
        self.prot.dataReceived('\x25\x60\x95\x13' \
            + '\x00\x00\x00\x00' \
            + 'Duisburg' \
            + '\x00\x00\x00\x00\x00\x00\x00\x04' \
            + '\x00\x00\x00\x05')
        self.assertEquals(
            '\x67\x44\x66\x98' \
            + '\x00\x00\x00\x00' \
            + 'Duisburg'
            + 'EFGHI', 
            str(self.dt))
        




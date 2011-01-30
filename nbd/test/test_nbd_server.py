import struct
from twisted.trial import unittest

from nbd.nbd import NBDServerProtocol

class DummyTransport(object):
    def __init__(self):
        self.writtens = [ ]
        self.connectionLost = False
    def write(self, s):
        self.writtens.append(s)
    def reset(self):
        self.writtens = []
    def loseConnection(self):
        self.connectionLost = True
    def __str__(self):
        return ''.join(self.writtens)

class StringBlockDevice(object):
    '''
    String posing as block device
    '''
    def __init__(self, s):
        self.s = s
    def sizeBytes(self):
        return len(self.s)
    def read(self, offset, length):
        assert offset >= 0
        assert offset + length <= len(self.s)
        return self.s[offset:offset+length] 
    def write(self, offset, payload):
        assert offset >= 0
        assert offset + len(payload) <= len(self.s)
        self.s = self.s[:offset] + payload + self.s[offset+len(payload):]
    def __str__(self):
        return self.s

REQUEST_MAGIC = '\x25\x60\x95\x13'
RESPONSE_MAGIC = '\x67\x44\x66\x98'

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
        self.prot.dataReceived(REQUEST_MAGIC \
            + '\x00\x00\x00\x00' \
            + 'Duisburg' \
            + '\x00\x00\x00\x00\x00\x00\x00\x04' \
            + '\x00\x00\x00\x05')
        self.assertEquals(
            RESPONSE_MAGIC \
            + '\x00\x00\x00\x00' \
            + 'Duisburg'
            + 'EFGHI', 
            str(self.dt))
        
    def test_split_read_request(self):
        self.prot.connectionMade()
        self.dt.reset()
        self.prot.dataReceived(REQUEST_MAGIC \
            + '\x00\x00\x00\x00Duis')
        self.prot.dataReceived(
             'burg' \
            + '\x00\x00\x00\x00\x00\x00\x00\x04' \
            + '\x00\x00\x00')
        self.prot.dataReceived('\x05')
        self.assertEquals(
            RESPONSE_MAGIC \
            + '\x00\x00\x00\x00' \
            + 'Duisburg'
            + 'EFGHI', 
            str(self.dt))

    def test_valid_write_request(self):
        self.prot.connectionMade()
        self.dt.reset()
        self.prot.dataReceived(REQUEST_MAGIC
            + '\x00\x00\x00\x01'
            + 'Hannover'
            + '\x00\x00\x00\x00\x00\x00\x00\x03'
            + '\x00\x00\x00\x04'
            + 'wxyz')
        self.assertEquals(RESPONSE_MAGIC + '\x00\x00\x00\x00' + 'Hannover',
            str(self.dt))
        self.assertEquals('ABCwxyzHIJKL', str(self.bd))

    def test_split_write_request(self):
        self.prot.connectionMade()
        self.dt.reset()
        self.prot.dataReceived(REQUEST_MAGIC
            + '\x00\x00\x00\x01'
            + 'Hannover'
            + '\x00\x00\x00\x00\x00\x00\x00\x03'
            + '\x00\x00\x00\x04'
            + 'w')
        self.prot.dataReceived('x')
        self.prot.dataReceived('yz')
        self.assertEquals(RESPONSE_MAGIC + '\x00\x00\x00\x00' + 'Hannover',
            str(self.dt))
        self.assertEquals('ABCwxyzHIJKL', str(self.bd))

    def test_multiple_queued_write_requests(self):
        self.prot.connectionMade()
        self.dt.reset()
        self.prot.dataReceived(
            REQUEST_MAGIC
            + '\x00\x00\x00\x01'
            + 'Hannover'
            + '\x00\x00\x00\x00\x00\x00\x00\x09'
            + '\x00\x00\x00\x02'
            + 'st'
            + REQUEST_MAGIC
            + '\x00\x00\x00\x01'
            + 'Budapest'
            + '\x00\x00\x00\x00\x00\x00\x00\x03'
            + '\x00\x00\x00\x04'
            + 'wxyz')
        self.assertEquals(RESPONSE_MAGIC + '\x00\x00\x00\x00' + 'Hannover' \
            + RESPONSE_MAGIC + '\x00\x00\x00\x00' + 'Budapest',
            str(self.dt))
        self.assertEquals('ABCwxyzHIstL', str(self.bd))


    def test_close(self):
        self.prot.connectionMade()
        self.dt.reset()
        self.prot.dataReceived(REQUEST_MAGIC
            + '\x00\x00\x00\x02'
            + 'Augsburg'
            + '\x00\x00\x00\x00\x00\x00\x00\x00'
            + '\x00\x00\x00\x00')
        self.assertEquals('', str(self.dt))
        self.assertTrue(self.dt.connectionLost)

    def test_read_error_read_error_in_first_blockdev_read(self):
        self.prot.connectionMade()
        self.dt.reset()
        def f(*args,**kwargs):
            raise IOError(99, 'Foo error')
        self.bd.read = f
        self.prot.dataReceived(REQUEST_MAGIC 
            + '\x00\x00\x00\x00'
            + 'Leberkas'
            + '\x00\x00\x00\x00\x00\x00\x00\x10'
            + '\x00\x00\x00\x01')
        resp = str(self.dt)
        a,b,c = struct.unpack('>4sI8s', resp)
        self.assertEquals(RESPONSE_MAGIC, a)
        self.assertEquals('Leberkas', c)
        self.assertEquals(99, b)

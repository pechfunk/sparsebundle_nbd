import struct
from twisted.trial import unittest
from twisted.test.proto_helpers import StringTransport

from nbd.nbd import NBDServerProtocol

class StringBlockDevice(object):
    '''
    String posing as block device
    '''
    def __init__(self, s):
        self.s = s
        self.stutterMode = False
    def sizeBytes(self):
        return len(self.s)
    def read(self, offset, length):
        assert offset >= 0
        assert offset + length <= len(self.s)
        result = self.s[offset:offset+length] 
        if self.stutterMode:
            for c in result:
                yield c
        else:
            yield result
    def write(self, offset, payload):
        assert offset >= 0
        assert offset + len(payload) <= len(self.s)
        self.s = self.s[:offset] + payload + self.s[offset+len(payload):]
    def __str__(self):
        return self.s

class FailAfterWrapper(object):
    def __init__(self, f, numGoodCalls, exc, args):
        self.f = f
        self.numGoodCalls = numGoodCalls
        self.callsSoFar = 0
        self.exc = exc
        self.excArgs = args

    def __call__(self, *a, **kwa):
        self.callsSoFar += 1
        if self.callsSoFar > self.numGoodCalls:
            raise (self.exc)(*self.excArgs)
        else:
            return (self.f)(*a,**kwa)

class GeneratorFailAfterWrapper(object):
    def __init__(self, f, numGoodCalls, exc, args):
        self.f = f
        self.numGoodCalls = numGoodCalls
        self.exc = exc
        self.excArgs = args
    def __call__(self, *a, **kwa):
        gen = (self.f)(*a, **kwa)
        i = 0
        for x in gen:
            i += 1
            if i > self.numGoodCalls:
                raise (self.exc)(*self.excArgs)
            else:
                yield x
                

REQUEST_MAGIC = '\x25\x60\x95\x13'
RESPONSE_MAGIC = '\x67\x44\x66\x98'

class NBDServerTest(unittest.TestCase):
    def setUp(self):
        self.bd = bd = StringBlockDevice('ABCDEFGHIJKL')
        self.prot = prot = NBDServerProtocol( bd )
        self.dt = dt = StringTransport()
        prot.makeConnection(dt)

    def test_welcome_handshake(self):
        self.assertEquals( 
            'NBDMAGIC' \
            + '\x00\x00\x42\x02\x81\x86\x12\x53' \
            + '\0\0\0\0\0\0\0\x0c' \
            + '\0' * 124, self.dt.value())

    def test_valid_read_request(self):
        self.dt.clear()
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
            self.dt.value())
        
    def test_split_read_request(self):
        self.dt.clear()
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
            self.dt.value())

    def test_valid_write_request(self):
        self.dt.clear()
        self.prot.dataReceived(REQUEST_MAGIC
            + '\x00\x00\x00\x01'
            + 'Hannover'
            + '\x00\x00\x00\x00\x00\x00\x00\x03'
            + '\x00\x00\x00\x04'
            + 'wxyz')
        self.assertEquals(RESPONSE_MAGIC + '\x00\x00\x00\x00' + 'Hannover',
            self.dt.value())
        self.assertEquals('ABCwxyzHIJKL', str(self.bd))

    def test_split_write_request(self):
        self.dt.clear()
        self.prot.dataReceived(REQUEST_MAGIC
            + '\x00\x00\x00\x01'
            + 'Hannover'
            + '\x00\x00\x00\x00\x00\x00\x00\x03'
            + '\x00\x00\x00\x04'
            + 'w')
        self.prot.dataReceived('x')
        self.prot.dataReceived('yz')
        self.assertEquals(RESPONSE_MAGIC + '\x00\x00\x00\x00' + 'Hannover',
            self.dt.value())
        self.assertEquals('ABCwxyzHIJKL', str(self.bd))

    def test_multiple_queued_write_requests(self):
        self.dt.clear()
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
            self.dt.value())
        self.assertEquals('ABCwxyzHIstL', str(self.bd))


    def test_close(self):
        self.dt.clear()
        self.prot.dataReceived(REQUEST_MAGIC
            + '\x00\x00\x00\x02'
            + 'Augsburg'
            + '\x00\x00\x00\x00\x00\x00\x00\x00'
            + '\x00\x00\x00\x00')
        self.assertEquals('', self.dt.value())
        self.assertTrue(self.dt.disconnecting)

    def test_read_error_read_error_in_first_blockdev_read(self):
        self.dt.clear()
        def f(*args,**kwargs):
            raise IOError(99, 'Foo error')
        self.bd.read = f
        self.prot.dataReceived(REQUEST_MAGIC 
            + '\x00\x00\x00\x00'
            + 'Leberkas'
            + '\x00\x00\x00\x00\x00\x00\x00\x10'
            + '\x00\x00\x00\x01')
        resp = self.dt.value()
        a,b,c = struct.unpack('>4sI8s', resp)
        self.assertEquals(RESPONSE_MAGIC, a)
        self.assertEquals('Leberkas', c)
        self.assertEquals(99, b)

    def test_read_error_read_error_in_second_blockdev_read(self):
        self.dt.clear()
        self.bd.stutterMode = True
        self.bd.read = GeneratorFailAfterWrapper(self.bd.read, 1, IOError, (98, 'wuff'))
        self.prot.dataReceived(REQUEST_MAGIC 
            + '\x00\x00\x00\x00'
            + 'Leberkas'
            + '\x00\x00\x00\x00\x00\x00\x00\x00'
            + '\x00\x00\x00\x03')
        resp = self.dt.value()
        self.assertEquals(struct.pack('>4sI8s', RESPONSE_MAGIC, 98, 'Leberkas'),
            resp)

class FailAfterWrapperTest(unittest.TestCase):
    def test_fails_after_n_times(self):
        def g(x):
            return 2*x
        gg = FailAfterWrapper(g, 1, IOError, (98, 'wuff'))
        self.assertEquals(18, gg(9))
        try:
            gg(10)
            self.fail()
        except IOError,e:
            self.assertEquals(98, e.errno)
    def test_generator_fails_after_n_times(self):
        def h(x):
            yield x+1
            yield x+2
            yield x+3
        gg = GeneratorFailAfterWrapper(h, 1, IOError, (98, 'wuff'))
        s = gg(5)
        self.assertEquals(6, s.next())
        try:
            s.next()
            self.fail()
        except IOError,e:
            self.assertEquals(98, e.errno)



'''
NBD protocol for Twisted.
'''

from twisted.internet import protocol
from twisted.internet import defer
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
import struct

class StringBlockDevice(object):
    '''
    String posing as block device
    '''
    def __init__(self, s):
        self.s = s
    def sizeBytes(self):
        return len(self.s)
    def read(self, offset, length):
        return self.s[offset:offset+length] #TODO check offset and length
    def write(self, offset, payload):
        raise Exception("Not implemented")

class Error(Exception):
    pass

class ReadRequest(object):
    def __init__(self, blockdev, transport, handle, offset, length):
        self.handle = handle
        self.offset = offset
        self.length = length
        self.transport = transport
        self.blockdev = blockdev
    def run(self):
        self._answerReadResponseHeader()
        for s in self.blockdev.read(self.offset, self.length):
            self.transport.write(s)
    def _answerReadResponseHeader(self):
        assert type(self.handle) is type('') and len(self.handle) == 8
        msg = '\x67\x44\x66\x98' + struct.pack('>L', 0) + self.handle 
        self.transport.write(msg)
    def _answerReadError(self, *args):
        raise Exception("not implemented")
        
class WriteRequest(object):
    def __init__(self, blockdev, handle, offset, length, payload):
        self.handle = handle
        self.offset = offset
        self.length = length
        self.payload = payload
    def run(self):
        self.blockdev.write(self.offset, self.payload)

class DisconnectRequest(object):
    def __init__(self, transport):
        self.transport = transport
    def run(self):
        self.transport.loseConnection()
        
class NBDServerProtocol(protocol.Protocol):
    '''
    I am the server side of one NBD connection.
    '''

    SERVER_MAGIC = 'NBDMAGIC' + '\x00\x00\x42\x02\x81\x86\x12\x53' 
    REQUEST_TEMPLATE = '>LL8sQL'
    REQUEST_HEADER_SIZE = struct.calcsize(REQUEST_TEMPLATE)
    REQUEST_MAGIC = 0x25609513
    CMD_READ = 0
    CMD_WRITE = 1
    CMD_DISCONNECT = 2
    
    def __init__(self, blockdev):
        '''
        Constructor
        '''
        self._blockdev = blockdev
        self._readBuffer = ''
        
    def connectionMade(self):
        size = self._blockdev.sizeBytes()
        self.transport.write(self.SERVER_MAGIC + struct.pack('>Q', size) + '\0' * 124)
        
    def dataReceived(self, bs):
        self._readBuffer = self._readBuffer + bs
        numBytesUsed, request = self.parseRequest(self._readBuffer)
        if request is not None :
            self._readBuffer = self._readBuffer[numBytesUsed:]
            request.run()
        
    def parseRequest(self, msgBytes):
        """
        Try to interpret the bytes as a request, and return a pair
        of length (total length of the parsed request including payload) 
        and processed request. If the bytes correspond to 
        an incomplete request, return length 0 and request None.
        """
        if len(msgBytes) >= self.REQUEST_HEADER_SIZE :
            (magic, requestType, handle, offset, length) = struct.unpack_from(self.REQUEST_TEMPLATE, msgBytes)
            if magic != self.REQUEST_MAGIC: 
                raise Error(self.magic)
            if requestType == self.CMD_READ:
                return (self.REQUEST_HEADER_SIZE, 
                        ReadRequest(self._blockdev, self.transport, handle, offset, length))
            elif requestType == self.CMD_WRITE:
                if length + self.REQUEST_HEADER_SIZE >= len(msgBytes):
                    payload = msgBytes[self.REQUEST_HEADER_SIZE
                                       : self.REQUEST_HEADER_SIZE+length]
                    return (self.REQUEST_HEADER_SIZE + length, 
                            WriteRequest(self._blockdev, handle, offset, length, payload))
                else:
                    return (0, None)
            elif requestType == self.CMD_DISCONNECT:
                return (self.REQUEST_HEADER_SIZE, DisconnectRequest())
            else:
                raise Error(requestType)
        else:
            return (0, None)
            

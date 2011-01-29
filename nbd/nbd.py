'''
NBD protocol for Twisted.
'''

from twisted.internet import protocol
from twisted.internet import defer
from twisted.python import log
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
import struct

SERVER_MAGIC = 'NBDMAGIC' + '\x00\x00\x42\x02\x81\x86\x12\x53' 
REQUEST_TEMPLATE = '>LL8sQL'
REQUEST_HEADER_SIZE = struct.calcsize(REQUEST_TEMPLATE)
REQUEST_MAGIC = 0x25609513
CMD_READ = 0
CMD_WRITE = 1
CMD_DISCONNECT = 2

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

class Error(Exception):
    pass

class BaseState(object):
    def __init__(self, transport, blockdev):
        self.transport = transport
        self.blockdev = blockdev

    def _writeResponseHeader(self, errCode, handle):
        assert type(handle) is type('') and len(handle) == 8
        msg = '\x67\x44\x66\x98' + struct.pack('>L', errCode) + handle 
        self.transport.write(msg)




        
class WriteState(BaseState):
    def __init__(self, blockdev, transport, handle, offset, length):
        super(WriteState,self).__init__(blockdev=blockdev, transport=transport)
        self.handle = handle
        self.offset = offset
        self.remainingLength = length

    def dataReceived(self, bs):
        if self.remainingLength < len(bs):
            data = bs[:self.remainingLength]
            state = ReadyState(transport=self.transport, blockdev=self.blockdev)
            bytesRead = self.remainingLength
        else:
            data = bs
            state = self
            bytesRead = len(bs)

        self.blockdev.write(self.offset, data)
        self.remainingLength -= bytesRead
        self.offset += bytesRead

        if self.remainingLength == 0:
            self._writeResponseHeader(0, self.handle)

        return bytesRead, state
        
class ReadyState(BaseState):

    def __init__(self, blockdev, transport):
        super(ReadyState, self).__init__(blockdev=blockdev, transport=transport)
        self._readBuffer = ''

    def dataReceived(self, bs):
        self._readBuffer = self._readBuffer + bs
        if len(self._readBuffer) >= REQUEST_HEADER_SIZE :
            (magic, requestType, handle, offset, length) = \
                 struct.unpack_from(REQUEST_TEMPLATE, self._readBuffer)
            unusedSize = len(self._readBuffer) - REQUEST_HEADER_SIZE
            numBytesRead = len(bs) - unusedSize
            if magic != REQUEST_MAGIC: 
                raise Error(magic)

            if requestType == CMD_READ:
                self._read(handle, offset, length)
                self._readBuffer = ''
                return (numBytesRead, self)

            elif requestType == CMD_WRITE:
                return (numBytesRead,
                    WriteState(transport=self.transport, 
                        blockdev=self.blockdev, handle=handle, offset=offset, length=length))

            elif requestType == CMD_DISCONNECT:
                self.transport.loseConnection()
                return (numBytesRead, self)

            else:
                raise Error(requestType)
        else:
            return (len(bs), self)
            
    def _read(self, handle, offset, length):
        self._writeResponseHeader(0, handle)
        for s in self.blockdev.read(offset, length):
            self.transport.write(s)



class NBDServerProtocol(protocol.Protocol):
    '''
    I am the server side of one NBD connection.
    '''

    
    def __init__(self, blockdev):
        '''
        Constructor
        '''
        self.blockdev = blockdev
        
    def connectionMade(self):
        size = self.blockdev.sizeBytes()
        self.transport.write(SERVER_MAGIC + struct.pack('>Q', size) + '\0' * 124)
        self.state = ReadyState(transport = self.transport, blockdev = self.blockdev)
        
    def dataReceived(self, bs):
        bytesRead = 0
        while bs != '':
            bytesRead, self.state = self.state.dataReceived(bs)
            bs = bs[bytesRead:]

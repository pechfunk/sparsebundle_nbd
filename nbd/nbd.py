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

        try:
            self.blockdev.write(self.offset, data)
            self.remainingLength -= bytesRead
            self.offset += bytesRead

            if self.remainingLength == 0:
                self._writeResponseHeader(0, self.handle)
        except IOError, e:
            self._writeResponseHeader(e.errno, self.handle)
            state = ReadyState(transport = self.transport, blockdev = self.blockdev)

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
        try:
            # I have to read all segments in advance so that I know what
            # error code to put into the response header.
            segs = list(self.blockdev.read(offset, length))
            self._writeResponseHeader(0, handle)
            for seg in segs:
                self.transport.write(seg)
        except IOError, e:
            self._writeResponseHeader(e.errno, handle)



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

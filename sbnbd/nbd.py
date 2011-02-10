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
    """
    State pattern for NBD servers. Base class for states.

    @ivar transport the transport to send responses on

    @iver blockdev the blockdev which does the file IO for us
    """
    def __init__(self, transport, blockdev):
        self.transport = transport
        self.blockdev = blockdev

    def _writeResponseHeader(self, errCode, handle):
        "Write a response header with errCode and handle"
        assert type(handle) is type('') and len(handle) == 8
        msg = '\x67\x44\x66\x98' + struct.pack('>L', errCode) + handle 
        self.transport.write(msg)
        
    def dataReceived(self, bs):
        """
        Some bytes have come from the network. Act accordingly.
        Return a pair (n, st) where n is the number of bytes in bs
        I have consumed, and st is the next state.
        """
        raise NotImplementedError()
        
class WriteState(BaseState):
    """
    The state in which the header of a write request has promised bytes
    which have not yet completely arrived, so the next N bytes should
    be written to the file; the rest is the next header.

    @ivar remainingLength how many more bytes of payload do we expect

    @ivar handle request handle

    @ivar offset within the blockdev to which I seek before writing
    """
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
    """
    The state in which the next bytes from the wire are part of a request.

    @ivar _readBuffer a growing request header
    """

    def __init__(self, blockdev, transport):
        super(ReadyState, self).__init__(blockdev=blockdev, transport=transport)
        self._readBuffer = ''

    def dataReceived(self, bs):
        # More data. Nice. Enough for a header?
        self._readBuffer = self._readBuffer + bs
        if len(self._readBuffer) >= REQUEST_HEADER_SIZE :
            # Parse the header and act
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

    @ivar blockdev a block device, e.g. BandBlockDevice. If None,
           I have to ask my factory for its .blockdev.

    @ivar state state pattern, see BaseState
    '''

    
    def __init__(self, blockdev = None):
        '''
        Constructor. If blockdev is not None, use it; else ask the factory.
        Supplying a blockdev is for tests.
        '''
        self.blockdev = blockdev
        
    def connectionMade(self):
        "Connection made. Send a greeting."
        blockdev = self._getBlockdev()
        size = blockdev.sizeBytes()
        self.transport.write(SERVER_MAGIC + struct.pack('>Q', size) + '\0' * 124)
        self.state = ReadyState(transport = self.transport, blockdev = blockdev)
        
    def dataReceived(self, bs):
        "Delegate bytes to state"
        bytesRead = 0
        while bs != '':
            bytesRead, self.state = self.state.dataReceived(bs)
            bs = bs[bytesRead:]

    def _getBlockdev(self):
        "find the blockdev, either in my fields or in my factory's"
        bd = self.blockdev
        if bd is None:
            bd = self.factory.blockdev
            assert bd is not None
        return bd

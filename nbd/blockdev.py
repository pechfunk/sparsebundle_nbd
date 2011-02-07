import os.path
import os
from StringIO import StringIO
import errno
import stat

'''
Block devices
'''
class BlockDeviceException(IOError):
    '''
    A BlockDevice could not serve a request because the request
    was invalid. 
    '''
    def __init__(self, msg):
        super(BlockDeviceException, self).__init__(errno.EINVAL, msg)


class BandBlockDevice(object):
    '''
    Simulate the contiguous block device using a directory of
    bands, files of fixed size.
    
    @ivar numBands: the number of bands
    
    @ivar bandSize: the size of the bands in bytes.
    
    @ivar bandFileFactory: gives me a file-like for a band number.  
    '''
    def __init__(self, numBands, bandSize, bandFileFactory):
        self.numBands = numBands
        self.bandSize = bandSize
        assert self.bandSize > 0
        self.size = numBands * bandSize
        self.bandFileFactory = bandFileFactory

    def sizeBytes(self):
        'the total size in bytes.'
        return self.size

    def read(self, offset, size):
        "Read size bytes from the volume, starting at volume offset offset. Generator for strings."
        if offset < 0:
            raise BlockDeviceException('negative offset: '+offset)
        if size < 0:
            raise BlockDeviceException('negative size')
        if offset+size > self.size :
            raise BlockDeviceException('attempted to read past end of sparse bundle')
        
        i = offset / self.bandSize
        o = offset % self.bandSize
        remSize = size
        while remSize > 0:
            f = self.bandFileFactory.getBand(i)
            f.seek(o, os.SEEK_SET)
            if o + remSize > self.bandSize:
                s = self.bandSize - o
            else:
                s = remSize
            remSize -= s
            yield f.read(s) #TODO may legally read less than s
            o = 0
            i += 1

    def write(self, offset, data):
        if offset < 0:
            raise BlockDeviceException('negative offset: '+offset)
        if offset + len(data) > self.size:
            raise BlockDeviceException('attempted to write past end of sparse bundle')

        i = offset / self.bandSize
        o = offset % self.bandSize
        remSize = len(data)
        so = 0
        while remSize > 0:
            f = self.bandFileFactory.getBand(i)
            f.seek(o, 0)
            if o + remSize > self.bandSize:
                s = self.bandSize - o
            else:
                s = remSize
            remSize -= s
            f.write(data[so : so+s])
            so += s
            o = 0
            i += 1

class AbstractPaddedFile(object):
    def __init__(self, realSize, virtSize ):
        self.realSize = realSize
        self.virtSize = virtSize

    def read(self, size):
        """
        Read bytes. Must give the expected size.
        """
        s, pos = self._innerRead(size)
        if len(s) < size:
            # the file read fewer bytes than the caller wanted.
            # Either the read produced fewer bytes than expected
            # (which read may do), or we've reached the end.
            if pos >= self.realSize:
                # We owe the caller NULs all the way from the current
                # position to the declared end.
                #import pdb ; pdb.set_trace()
                maxPad = self.virtSize - pos   # max that many bytes of padding
                missing = size - len(s)    # how many NULs the caller expects
                padSize = min(missing, maxPad)
                s = s + (padSize * '\0')
            else:
                # just an ordinary read returning less than expected. 
                pass
        return s

    def seek(self, pos, whence=os.SEEK_SET):
        self._doSeek(pos, whence)
    
    def _innerRead(self, size):
        raise NotImplementedError()

    def _doSeek(self, pos, whence):
        raise NotImplementedError()

class PaddedFile(AbstractPaddedFile):
    """
    Wrap a file, pretending it has been NUL-padded to a certain size.
    """
    def __init__(self, f, realSize, virtSize):
        super(PaddedFile, self).__init__(realSize, virtSize)
        self.f = f
        self.pos = 0

    def _innerRead(self, size):
        s = self.f.read(size)
        self.pos += len(s)
        return (s, self.pos)

    def _doSeek(self, pos, whence):
        self.f.seek(pos, whence)
        self.pos = pos

    def tell(self):
        return self.pos
                
class FixedSizeEmptyReadOnlyFile(AbstractPaddedFile):
    def __init__(self, size):
        super(FixedSizeEmptyReadOnlyFile, self).__init__(0, size)
        self.pos = 0
        self.hasSeekedSinceLastRead = True

    def _innerRead(self, size):
        assert self.hasSeekedSinceLastRead
        self.hasSeekedSinceLastRead = False
        return ('', self.pos)
    
    def _doSeek(self, pos, whence):
        assert whence == os.SEEK_SET
        self.pos = pos
        self.hasSeekedSinceLastRead = True
    def tell(self):
        return self.pos

def fileSize(f):
    st = os.stat(f)
    return st[stat.ST_SIZE]


class BandFileFactory(object):
    """
    Find bands in an Apple-like bands directory.
    Band numbers are hex numbers without leading 0s.
    """
    def __init__(self, dirName, bandSize, writable=False, fileCtor=file, fileSize=fileSize):
        self.fileCtor = fileCtor
        self.fileSize = fileSize
        self.bandSize = bandSize
        self.dirName = dirName
        if writable:
            self.openMode = 'r+b'
        else:
            self.openMode = 'rb'
        
    def getBand(self, index):
        name = "%x"%index
        fullName = os.path.join(self.dirName, name)
        try:
            f =  (self.fileCtor)(fullName, self.openMode)
            realSize = (self.fileSize)(fullName)
            wf =  PaddedFile(f, realSize, self.bandSize) 
        except IOError, e:
            if e.errno == errno.ENOENT:
                wf = FixedSizeEmptyReadOnlyFile(self.bandSize)
            else:
                raise
        return wf
        


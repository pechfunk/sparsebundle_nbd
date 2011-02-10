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

    @ivar lastBandSize: the size of the last band in bytes

    @ivar size: the size of the entire device in bytes
    
    @ivar bandFileFactory: gives me a file-like for a band number.  
    '''
    def __init__(self, totalSize, bandSize, bandFileFactory):
        self.numBands = (totalSize + bandSize - 1) / bandSize
        self.size = totalSize
        self.bandSize = bandSize
        self.lastBandSize = totalSize - (self.numBands-1)*bandSize
        assert self.bandSize > 0
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
            f = self._getBand(i)
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
        "write the data to the given offset"
        if offset < 0:
            raise BlockDeviceException('negative offset: '+offset)
        if offset + len(data) > self.size:
            raise BlockDeviceException('attempted to write past end of sparse bundle')

        i = offset / self.bandSize
        o = offset % self.bandSize
        remSize = len(data)
        so = 0
        while remSize > 0:
            f = self._getBand(i)
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

    def _getBand(self, i):
        "Get a filelike for the ith band"
        if i < self.numBands - 1:
            f = self.bandFileFactory.getBand(i, self.bandSize)
        elif i == self.numBands - 1:
            f = self.bandFileFactory.getBand(i, self.lastBandSize)
        else:
            raise AssertionError("invalid band index "+i)
        return f


class AbstractPaddedFile(object):
    """
    Superclass for file-likes with a fixed virtual size, either backed by
    another file, or entirely simulated. Subclasses must override _innerRead
    and _doSeek.
    """
    def __init__(self, realSize, virtSize ):
        "Init with a backing file of size realSize, reading NULs up to virtSize"
        self.realSize = realSize
        self.virtSize = virtSize

    def read(self, size):
        """
        Read bytes. Must give the expected size.
        May return fewer bytes, just like read() calls are wont to do.
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
        "Seek to a position. Only SEEK_SET supported."
        self._doSeek(pos, whence)
    
    def _innerRead(self, size):
        "Read up to that many bytes from current position. Return (buf, pos_after)"
        raise NotImplementedError()

    def _doSeek(self, pos, whence):
        "Seek to that position. Only SEEK_SET supported."
        raise NotImplementedError()

class PaddedFile(AbstractPaddedFile):
    """
    Wrap a file, pretending it has been NUL-padded to a certain size.
    """
    def __init__(self, f, realSize, virtSize):
        """
        Wrap file f. It is believed to have size realSize. Pretend it
        is virtSize bytes long by appending NULs.
        """
        super(PaddedFile, self).__init__(realSize, virtSize)
        self.f = f
        self.pos = 0

    def _innerRead(self, size):
        "read from the inner file"
        s = self.f.read(size)
        self.pos += len(s)
        return (s, self.pos)

    def _doSeek(self, pos, whence):
        "seek"
        self.f.seek(pos, whence)
        self.pos = pos

    def tell(self):
        "current position, as in files"
        return self.pos
                
class FixedSizeEmptyReadOnlyFile(AbstractPaddedFile):
    """
    A file-like which behaves like an empty read-only file of 
    a given number of NUL bytes.
    I still keep track of the current position.
    """
    def __init__(self, size):
        "Init with a given virtual size"
        super(FixedSizeEmptyReadOnlyFile, self).__init__(0, size)
        self.pos = 0
        self.hasSeekedSinceLastRead = True

    def _innerRead(self, size):
        "Just adjust the position."
        assert self.hasSeekedSinceLastRead
        self.hasSeekedSinceLastRead = False
        return ('', self.pos)
    
    def _doSeek(self, pos, whence):
        "Set the position"
        assert whence == os.SEEK_SET
        self.pos = pos
        self.hasSeekedSinceLastRead = True
    def tell(self):
        return self.pos

def fileSize(f):
    "Size of a file with name f"
    st = os.stat(f)
    return st[stat.ST_SIZE]


class BandFileFactory(object):
    """
    Find bands in an Apple-like bands directory.
    Band numbers are hex numbers without leading 0s.
    """
    def __init__(self, dirName, writable=False, fileCtor=file, fileSize=fileSize):
        """
        New instance. dirName is the name of the directory containing the
        Info.plist file (not the bands directory!). writable makes the file
        writable, default is read-only. fileCtor is for testing (factory
        for file-likes). fileSize is for testing (given a filename, return
            its size)
        """
        self.fileCtor = fileCtor
        self.fileSize = fileSize
        self.dirName = dirName
        if writable:
            self.openMode = 'r+b'
        else:
            self.openMode = 'rb'
        
    def getBand(self, index, virtualSize):
        """Get the band with the given index, and wrap it to behave 
        as if it had size virtualSize"""
        name = "%x"%index
        fullName = os.path.join(self.dirName, name)
        try:
            f =  (self.fileCtor)(fullName, self.openMode)
            realSize = (self.fileSize)(fullName)
            wf =  PaddedFile(f, realSize, virtualSize) 
        except IOError, e:
            if e.errno == errno.ENOENT:
                wf = FixedSizeEmptyReadOnlyFile(virtualSize)
            else:
                raise
        return wf
        


import os.path
import os
from StringIO import StringIO
import errno

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
            f.seek(o, 0)
            if o + remSize > self.bandSize:
                s = self.bandSize - o
            else:
                s = remSize
            remSize -= s
            yield f.read(s)
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






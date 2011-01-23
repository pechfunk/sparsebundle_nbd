'''
Block devices
'''
class BlockDeviceException(Exception):
    '''
    A BlockDevice could not serve a request because the request
    was invalid. 
    '''

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
        if offset < 0:
            raise BlockDeviceException('negative offset: '+offset)
        if size < 0:
            raise BlockDeviceException('negative size')

        if offset+size > self.size :
            raise BlockDeviceException('attempted to read past end of sparse bundle')
        bandIndex = offset / self.bandSize
        bandOffset = offset % self.bandSize
        if bandOffset+size > self.bandSize:
            raise NotImplementedError('not implemented: requests spanning')
        else:
            f = self.bandFileFactory.getBand(bandIndex)
            f.seek(bandOffset, 0)
            yield f.read(size)


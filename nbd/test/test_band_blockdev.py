'''
Created on 16.01.2011

@author: konrad
'''
from twisted.trial import unittest
try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO

from nbd.blockdev import BandBlockDevice, BlockDeviceException

class DummyFileFactory(object):
    def __init__(self, bandContents):
        self.numBands = len(bandContents)
        self.bands = [StringIO(x) for x in bandContents]
        
    def getBand(self, k):
        assert 0 <= k < self.numBands
        return self.bands[k]
    
    def extract(self):
        'a list, containing each band\'s contents in a string'
        return [str(x) for x in self.bands]

def y(strs):
    return ''.join(strs)

class BandBlockDeviceTest(unittest.TestCase):
    '''
    Unit test for BandBlockDevice
    '''

    BAND_SIZE=8
    
    def _makeBBD(self):
        "make a BandBlockDevice of ABCDEFGH abcdefgh 01234567"
        numBands = 3
        bandSize = self.BAND_SIZE
        dff = DummyFileFactory(['ABCDEFGH', 'abcdefgh', '01234567'])
        bd = BandBlockDevice(numBands, bandSize, dff)
        return bd


    def test_read_full_first_band(self):
        bd = self._makeBBD()
        s = bd.read(0, self.BAND_SIZE)
        self.assertEquals('ABCDEFGH', y(s))

    def test_read_full_middle_band(self):
        bd = self._makeBBD()
        s = bd.read(self.BAND_SIZE, self.BAND_SIZE)
        self.assertEquals('abcdefgh', y(s))

    def test_read_full_last_band(self):
        bd = self._makeBBD()
        s = bd.read(2*self.BAND_SIZE, self.BAND_SIZE)
        self.assertEquals('01234567', y(s))

    def test_read_part_of_middle_band(self):
        bd = self._makeBBD()
        s = bd.read(self.BAND_SIZE+3, 2)
        self.assertEquals('de', y(s))

    def test_error_read_overlaps_end(self):
        bd = self._makeBBD()
        try:
            s = bd.read(20, 5)
        except BlockDeviceException, e:
            pass

    def test_error_read_after_end(self):
        bd = self._makeBBD()
        try:
            s = bd.read(25, 1)
        except BlockDeviceException, e:
            pass

    def test_read_overlap_one_boundary_from_start(self):
        bd = self._makeBBD()
        s = bd.read(0, 10)
        self.assertEquals('ABCDEFGHab', y(s))

    def test_read_overlap_one_boundary(self):
        bd = self._makeBBD()
        s = bd.read(6, 4)
        self.assertEquals('GHab', y(s))

    def test_read_overlap_two_boundaries(self):
        bd = self._makeBBD()
        s = bd.read(6, 11)
        self.assertEquals('GHabcdefgh0', y(s))

        
    def test_size_bytes(self):
        bd = self._makeBBD()
        size = bd.sizeBytes()
        self.assertEquals(24, size)

    def test_y(self):
        "test the y helper"
        self.assertEquals('ABCdefGHI', y(x for x in ('ABC','def','GHI')))
    

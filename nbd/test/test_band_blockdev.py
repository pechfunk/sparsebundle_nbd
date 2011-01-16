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
        self.assertEquals('ABCDEFGH', s)

    def test_read_full_middle_band(self):
        bd = self._makeBBD()
        s = bd.read(self.BAND_SIZE, self.BAND_SIZE)
        self.assertEquals('abcdefgh', s)

    def test_read_full_last_band(self):
        bd = self._makeBBD()
        s = bd.read(2*self.BAND_SIZE, self.BAND_SIZE)
        self.assertEquals('01234567', s)

    def test_read_part_of_middle_band(self):
        bd = self._makeBBD()
        s = bd.read(self.BAND_SIZE+3, 2)
        self.assertEquals('de', s)

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
        
    def test_size_bytes(self):
        bd = self._makeBBD()
        size = bd.sizeBytes()
        self.assertEquals(24, size)
    

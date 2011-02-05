from errno import ENOENT
from os import SEEK_SET
from twisted.trial import unittest
from nbd.blockdev import BandFileFactory, FixedSizeEmptyReadOnlyFile
from StringIO import StringIO

class BandFileFactoryReadingTest(unittest.TestCase):
    def setUp(self):
        self.dirName = "/bla/"
        self.bandSize = 40
        self.fileOpenings = []
        self.pretendFileExists = True
        self.bff = BandFileFactory(self.dirName, bandSize=self.bandSize,
                                   writable=False, fileCtor=self.fakeFile)
        
    def fakeFile(self, filename, mode):
        self.fileOpenings.append((filename, mode))
        if self.pretendFileExists:
            return StringIO('hello '+filename)
        else:
            raise IOError(ENOENT, "No hay")

    def test_open_existing_band_0(self):
        f = self.bff.getBand(0)
        self.assertEquals(1, len(self.fileOpenings))
        good = 'hello /bla/0'
        s = f.read(len(good)+2)
        self.assertEquals(good+'\0\0', s)
        f.seek(6)
        self.assertEquals('/bla/0\0\0\0', f.read(9))
        f.seek(self.bandSize-4)
        self.assertEquals('\0'*4, f.read(4))

    def test_open_existing_band_31(self):
        f = self.bff.getBand(31)
        self.assertEquals([('/bla/1f', 'rb')], self.fileOpenings)

    def test_open_nonexisting_band_returns_all_zero_file(self):
        self.pretendFileExists = False
        f = self.bff.getBand(255)
        self.assertEquals(1, len(self.fileOpenings))
        self.assertEquals(('/bla/ff', 'rb'), self.fileOpenings[0])
        self.assertEquals('\0'*13, f.read(13))
        f.seek(self.bandSize - 5, SEEK_SET)
        self.assertEquals('\0'*5, f.read(5))

class FixedSizeEmptyReadOnlyFileTest(unittest.TestCase):
    def test_read_size_0(self):
        f = FixedSizeEmptyReadOnlyFile(0)
        s = f.read(5)
        self.assertEquals('',s)

    def test_read_all_from_beginning(self):
        f = FixedSizeEmptyReadOnlyFile(5)
        s = f.read(5)
        self.assertEquals('\0'*5, s)

    def test_read_some_from_beginning(self):
        f = FixedSizeEmptyReadOnlyFile(5)
        s = f.read(3)
        self.assertEquals('\0'*3, s)

    def test_seek_middle_read(self):
        f = FixedSizeEmptyReadOnlyFile(5)
        f.seek(1)
        s = f.read(2)
        self.assertEquals('\0'*2, s)

    def test_seek_end_read(self):
        f = FixedSizeEmptyReadOnlyFile(5)
        f.seek(5)
        s = f.read(2)
        self.assertEquals('', s)

    def test_seek_before_end_read(self):
        f = FixedSizeEmptyReadOnlyFile(5)
        f.seek(4)
        s = f.read(2)
        self.assertEquals('\0', s)
        

from errno import ENOENT
from os import SEEK_SET
from twisted.trial import unittest
from sbnbd.blockdev import BandFileFactory, FixedSizeEmptyReadOnlyFile,\
    PaddedFile
from StringIO import StringIO

class BandFileFactoryReadingTest(unittest.TestCase):
    """
    Unit test for BandFileFactory. 
    
    Test BandFileFactory and the wrapped files it returns.
    """
    def setUp(self):
        self.dirName = "/bla/"
        self.bandSize = 40
        self.fileOpenings = []
        self.pretendFileExists = True
        self.pretendFileIsFull = True
        self.bff = BandFileFactory(self.dirName, 
                                   writable=False, 
                                   fileCtor=self.fakeFile,
                                   fileSize = self.fakeFileSize)
        
    def fakeFile(self, filename, mode):
        "Stub for file constructor"
        self.fileOpenings.append((filename, mode))
        if self.pretendFileExists:
            if self.pretendFileIsFull:
                return StringIO(('hello '+filename).ljust(self.bandSize))
            else:
                return StringIO('hello')
        else:
            raise IOError(ENOENT, "No hay")

    def fakeFileSize(self, filename):
        "Stub for the function which computes file sizes"
        if self.pretendFileIsFull:
            return self.bandSize
        else:
            return 5

    def test_open_existing_band_0(self):
        f = self.bff.getBand(0, self.bandSize)
        self.assertEquals(1, len(self.fileOpenings))
        good = 'hello /bla/0'
        s = f.read(len(good)+2)
        self.assertEquals(good+'  ', s)
        f.seek(6)
        self.assertEquals('/bla/0   ', f.read(9))
        f.seek(self.bandSize-4)
        self.assertEquals(' '*4, f.read(4))

    def test_open_existing_band_31(self):
        f = self.bff.getBand(31, self.bandSize)
        self.assertEquals([('/bla/1f', 'rb')], self.fileOpenings)

    def test_open_nonexisting_band_returns_all_zero_file(self):
        self.pretendFileExists = False
        f = self.bff.getBand(255, self.bandSize)
        self.assertEquals(1, len(self.fileOpenings))
        self.assertEquals(('/bla/ff', 'rb'), self.fileOpenings[0])
        self.assertEquals('\0'*13, f.read(13))
        f.seek(self.bandSize - 5, SEEK_SET)
        self.assertEquals('\0'*5, f.read(5))

    def test_open_too_short_file(self):
        self.pretendFileIsFull = False
        f = self.bff.getBand(255, self.bandSize)
        self.assertEquals('hello\0', f.read(6))
        f.seek(self.bandSize - 5, SEEK_SET)
        self.assertEquals('\0'*5, f.read(5))

class FixedSizeEmptyReadOnlyFileTest(unittest.TestCase):
    """
    Unit test for FixedSizeEmptyReadOnlyFile
    """
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
        
class TricklingReadFileWrapper(object):
    """
    A file wrapper which simulates that read() returns fewer
    bytes than expected.
    """
    def __init__(self, f, readSize):
        self.f = f
        self.readSize = readSize
    def read(self, size=-1):
        if size < 0:
            realSize = self.readSize
        else:
            realSize = min(self.readSize, size)
        return self.f.read(realSize)
    def seek(self, pos, whence = SEEK_SET):
        self.f.seek(pos, whence)
    def tell(self):
        return self.f.tell()

class PaddedFileTest(unittest.TestCase):
    """
    Unit test for PaddedFile
    """
    def setUp(self):
        self.f = StringIO("0123456789")
        self.tfw = TricklingReadFileWrapper(self.f, 4)
        self.pf = PaddedFile(self.tfw, 10, 16)
    def test_read_begin_short(self):
        s = self.pf.read(3)
        self.assertEquals(s, "012")
    def test_read_begin_long(self):
        s = self.pf.read(5)
        self.assertEquals(s, "0123")
    def test_read_phys_end_short(self):
        self.pf.seek(9)
        s = self.pf.read(3)
        self.assertEquals(s, "9\0\0")
    def test_read_phys_end_long(self):
        self.pf.seek(9)
        s = self.pf.read(5)
        self.assertEquals(s, "9\0\0\0\0")
    def test_read_virt_end_short(self):
        self.pf.seek(12)
        s = self.pf.read(4)
        self.assertEquals(s, "\0\0\0\0")
    def test_read_virt_end_long(self):
        self.pf.seek(12)
        s = self.pf.read(5)
        self.assertEquals(s, "\0\0\0\0")
    def test_seek_tell(self):
        self.pf.seek(13)
        self.assertEquals(13, self.pf.tell())


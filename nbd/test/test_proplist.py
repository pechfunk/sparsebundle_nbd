from twisted.trial import unittest
from nbd import proplist
from StringIO import StringIO

class PropListTest(unittest.TestCase):
    def testParseTypical(self):
        text = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>CFBundleInfoDictionaryVersion</key>
	<string>6.0</string>
	<key>band-size</key>
	<integer>8388608</integer>
	<key>bundle-backingstore-version</key>
	<integer>1</integer>
	<key>diskimage-bundle-type</key>
	<string>com.apple.diskimage.sparsebundle</string>
	<key>size</key>
	<integer>40960000</integer>
</dict>
</plist>"""
        fi = StringIO(text)
        d = proplist.parse(fi)
        self.assertEquals("6.0", d['CFBundleInfoDictionaryVersion'])
        self.assertEquals(8388608, d['band-size'])
        self.assertEquals(1, d['bundle-backingstore-version'])
        self.assertEquals("com.apple.diskimage.sparsebundle", d['diskimage-bundle-type'])
        self.assertEquals(40960000, d['size'])

import os
import sys
from twisted.internet import protocol, reactor
from nbd.nbd import NBDServerProtocol
from nbd.blockdev import BandBlockDevice, BandFileFactory
from nbd.proplist import parse

class NBDFactory(protocol.ServerFactory):
    protocol = NBDServerProtocol
    def __init__(self, blockdev):
        self.blockdev = blockdev

def makeFactory(bundleDir):
    bundlePlist = os.path.join(bundleDir, "Info.plist")
    plistFile = file(bundlePlist, "rb")
    plistData = parse(plistFile)
    bandsDir = os.path.join(bundleDir, "bands")
    bandSizeB = plistData["band-size"]
    sizeK = plistData["size"]
    bff = BandFileFactory(bandsDir, writable=False)
    bd = BandBlockDevice( totalSize = sizeK*1024, bandSize = bandSizeB,
        bandFileFactory = bff) 
    numBands = bandSizeB / sizeK
    fac = NBDFactory(bd)
    return fac

def serve(bundleDir, port):
    factory = makeFactory(bundleDir)
    reactor.listenTCP(port, factory)
    reactor.run()

if __name__=="__main__":
    bundleDir = sys.argv[1]
    port = int(sys.argv[2])
    serve(bundleDir, port)

    

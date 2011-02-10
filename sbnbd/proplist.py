"""
Proplist parser
"""
from xml.etree import ElementTree as ET

def parse(filelike):
    """Parser for the proplist files MacOS X uses to describe sparsebundles.
    Not error-tolerant at all.
    """
    data = {}
    et = ET.parse(filelike)
    plist = et.getroot()
    assert plist.tag == 'plist'
    assert len(plist) == 1
    dic = plist[0]
    assert dic.tag == 'dict'
    lastKey = None
    for elem in dic:
        if elem.tag == "key":
            lastKey = elem.text
        elif elem.tag == "string":
            assert lastKey is not None
            data[lastKey] = elem.text
            lastKey = None
        elif elem.tag == "integer":
            assert lastKey is not None
            data[lastKey] = int(elem.text)
            lastKey = None
    return data


# This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild

import kaitaistruct
from kaitaistruct import KaitaiStruct, KaitaiStream, BytesIO
from utils.log import log as log
from core.settings import settings

if getattr(kaitaistruct, 'API_VERSION', (0, 9)) < (0, 9):
    raise Exception("Incompatible Kaitai Struct Python API: 0.9 or later is required, but you have %s" % (kaitaistruct.__version__))

class Sz16flag(KaitaiStruct):
    def __init__(self, _io, _parent=None, _root=None):
        self._io = _io
        self._parent = _parent
        self._root = _root if _root else self
        self._read()

    def _read(self):
        self.dvc_flag = self._io.read_u1()

    def from_file_to_dict(binfile):
        dev_mode = settings.DEV_MODE
        #log.debug(Sz16flag.from_file(binfile).dvc_flag)
        sz16flagdict = Sz16flag.from_file(binfile).__dict__.copy()
        keylst = list(sz16flagdict.keys()).copy()
        #log.debug(sz16flagdict)
        #log.debug(keylst)
        for k in ('_io', '_parent', '_root'):
            if k in sz16flagdict:
                del sz16flagdict[k]
        for key,value in sz16flagdict.items():
            if isinstance(value, bool):
                if value:
                    sz16flagdict[key] = 1
                else:
                    sz16flagdict[key] = 0
        return sz16flagdict

    def from_bytes_to_dict(bytesobj):
        dev_mode = settings.DEV_MODE
        sz16flagdict = Sz16flag.from_bytes(bytesobj).__dict__.copy()
        keylst = list(sz16flagdict.keys()).copy()
        for k in ('_io', '_parent', '_root'):
            if k in sz16flagdict:
                del sz16flagdict[k]
        for key,value in sz16flagdict.items():
            if isinstance(value, bool):
                if value:
                    sz16flagdict[key] = 1
                else:
                    sz16flagdict[key] = 0
        return sz16flagdict



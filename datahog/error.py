# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

class NoShard(Exception):
    pass

class Timeout(Exception):
    pass

class AliasInUse(Exception):
    pass

class NoObject(Exception):
    pass

class ReadOnly(Exception):
    pass

class BadContext(Exception):
    pass

class MissingParent(Exception):
    pass

class IsRoot(Exception):
    pass

class StorageClassError(TypeError):
    pass

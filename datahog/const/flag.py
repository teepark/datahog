# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

from . import context

META = {}


def set_flag(value, ctx):
    ''' create a constant for use in flags

    :param int value:
        the "bit position" of this flag. must be between 1 and 16.

    :parm int ctx:
        the context for which this flag value applies
    '''
    if ctx in META and value in META[ctx]:
        raise ValueError("duplicate flag value")

    if value < 1 or value > 16:
        raise ValueError("flag value outside of range (1, 16): %d" % value)

    if ctx not in context.META:
        raise ValueError("unrecognized context const: %r" % ctx)

    META.setdefault(ctx, set()).add(value)
    return value

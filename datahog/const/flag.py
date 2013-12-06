# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

from . import context

META = {}


def set_flag(title, value, ctx):
    ''' create a constant for use in flags

    :param str title:
        the constant name. will be made available as datahog.const.flag.NAME.

    :param int value:
        the "bit position" of this flag. must be between 1 and 16.

    :parm int ctx:
        the context for which this flag value applies
    '''
    if ctx in META:
        if value in META[ctx]:
            raise ValueError("duplicate flag values for context %s: %s, %s" %
                    (context.META[ctx][0], META[ctx][value], title))

    if value < 1 or value > 16:
        raise ValueError("flag value outside of range (1, 16): %d" % value)

    if ctx not in context.META:
        raise ValueError("unrecognized context const: %r" % ctx)

    meta = META.setdefault(ctx, {})
    meta[value] = title

    return value

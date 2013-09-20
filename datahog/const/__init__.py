# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

from . import context, flag, table


__all__ = ['context', 'flag', 'table', 'set_context', 'set_flag']


set_context = context.set_context
set_flag = flag.set_flag

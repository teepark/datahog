# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

import mummy

from . import context, flag, table
from .. import error


def ctx_tbl(ctx):
    "get the table a particular context is attached to"
    return context.META.get(ctx, (None, None))[1]


def ctx_base_ctx(ctx):
    "get the context of a context's base_id object"
    if ctx not in context.META:
        return None
    meta = context.META[ctx][2]

    if 'base_ctx' not in meta:
        return None
    return meta['base_ctx']


def ctx_base(ctx):
    "get (table, context) for a context's base"
    base_ctx = ctx_base_ctx(ctx)
    return ctx_tbl(base_ctx), base_ctx


def ctx_base_tblname(ctx):
    "return the table name for a context's base"
    base_ctx = ctx_base_ctx(ctx)
    base_tbl = ctx_tbl(base_ctx)
    return table.NAMES.get(base_tbl)


def ctx_rel_ctx(ctx):
    "return the table name for a context's rel_id"
    if ctx not in context.META:
        return None
    meta = context.META[ctx][2]

    if 'rel_ctx' not in meta:
        return None
    return meta['rel_ctx']


def ctx_rel(ctx):
    "get (table, context) for a context's rel"
    rel_ctx = ctx_rel_ctx(ctx)
    return ctx_tbl(rel_ctx), rel_ctx


def ctx_rel_tblname(ctx):
    "return the table name for a context's rel"
    rel_ctx = ctx_rel_ctx(ctx)
    rel_tbl = ctx_tbl(rel_ctx)
    return table.NAMES.get(rel_tbl)


def ctx_storage(ctx):
    "return the storage type for a context"
    meta = context.META.get(ctx)
    return meta and meta[2].get('storage')


def flags_to_int(ctx, flag_list):
    "convert an iterable of flag consts to a single bitmap integer"
    if ctx not in context.META:
        raise error.BadContext(ctx)

    num = 0
    for i in flag_list:
        if i not in flag.META.get(ctx, ()):
            raise error.BadFlag(i, ctx)
        num |= (1 << (i - 1))
    return num


def int_to_flags(ctx, flag_num):
    "convert a flags bitmap int to a set of flag consts"
    if ctx not in context.META:
        raise error.BadContext(ctx)

    meta = flag.META.get(ctx, {})
    flag_set = set()
    i = 1
    while flag_num:
        if flag_num & 1 and i in meta:
            flag_set.add(i)
        flag_num >>= 1
        i += 1
    return flag_set


def storage_wrap(ctx, value):
    st = ctx_storage(ctx)

    if st == context.STORAGE_NULL:
        if value is not None:
            raise error.StorageClassError("STORAGE_NULL requires None")
        return None

    if st == context.STORAGE_INT:
        if not isinstance(value, (int, long)):
            raise error.StorageClassError("STORAGE_INT requires int or long")
        return value

    if st == context.STORAGE_STR:
        if not isinstance(value, str):
            raise error.StorageClassError("STORAGE_STR requires str")
        return value

    if st == context.STORAGE_UTF8:
        if not isinstance(value, unicode):
            raise error.StorageClassError("STORAGE_UTF8 requires unicode")
        return value.encode("utf8")

    if st == context.STORAGE_SER:
        try:
            return mummy.dumps(value)
        except TypeError:
            raise error.StorageClassError(
                    "STORAGE_SER requires a serializable value")

    raise error.BadContext(ctx)


def storage_unwrap(ctx, value):
    st = ctx_storage(ctx)
    if st is None:
        raise error.BadContext(ctx)

    if st == context.STORAGE_UTF8:
        return st.decode("utf8")

    if st == context.STORAGE_SER:
        return mummy.loads(value)

    return value

# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

import mummy
import psycopg2

from . import context, flag, storage, table
from .. import error


def ctx_tbl(ctx):
    "get the table a particular context is attached to"
    return context.META.get(ctx, (None,))[0]


def ctx_base_ctx(ctx):
    "get the context of a context's base_id object"
    if ctx not in context.META:
        return None
    meta = context.META[ctx][1]

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
    meta = context.META[ctx][1]

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
    return meta and meta[1].get('storage')


def ctx_schema(ctx):
    "return the storage schema for a context (if present)"
    meta = context.META.get(ctx)
    return meta and meta[1].get('schema')


def ctx_search(ctx):
    "return the search class for a context (if present)"
    meta = context.META.get(ctx)
    return meta and meta[1].get('search')


def ctx_phonetic_loose(ctx):
    "return the 'phonetic_loose' context option"
    meta = context.META.get(ctx)
    return meta and meta[1].get('phonetic_loose')


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

    if st == storage.NULL:
        if value is not None:
            raise error.StorageClassError("NULL requires None")
        return None

    if st == storage.INT:
        if not isinstance(value, (int, long)):
            raise error.StorageClassError("INT requires int or long")
        return value

    if st == storage.STR:
        if not isinstance(value, str):
            raise error.StorageClassError("STR requires str")
        return psycopg2.Binary(value)

    if st == storage.UTF:
        if not isinstance(value, unicode):
            raise error.StorageClassError("UTF storage requires unicode")
        return value.encode("utf8")

    if st == storage.SERIAL:
        schema = ctx_schema(ctx)
        if schema:
            msg = schema(value)
            try:
                return msg.dumps()
            except schema.InvalidMessage:
                raise error.StorageClassError(
                        "SERIAL schema validation failed", msg.message)
        try:
            return mummy.dumps(value)
        except TypeError:
            raise error.StorageClassError(
                    "SERIAL requires a serializable value")


    raise error.BadContext(ctx)


def storage_unwrap(ctx, value):
    st = ctx_storage(ctx)
    if st is None:
        raise error.BadContext(ctx)

    if st == storage.STR:
        return str(value)

    if st == storage.UTF:
        return value.decode("utf8")

    if st == storage.SERIAL:
        schema = ctx_schema(ctx)
        if schema:
            return schema.untransform(mummy.loads(str(value)))
        return mummy.loads(str(value))

    return value


_dm = None

def dmetaphone(value):
    global _dm
    if _dm is None:
        import fuzzy
        _dm = fuzzy.DMetaphone()
    dm, dmalt = _dm(value)
    return dm.ljust(4, ' '), (dmalt.ljust(4, ' ') if dmalt else None)

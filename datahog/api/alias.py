# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

import hashlib

from .. import error
from ..const import table, util
from ..db import query, txn


__all__ = ['set', 'lookup', 'list', 'add_flags', 'clear_flags', 'remove']


def set(pool, base_id, ctx, value, flags=None, index=None, timeout=None):
    '''set an alias value on a guid object

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the alias's context

    :param str value: the alias value

    :param iterable flags:
        the flags to set in the event that the alias is newly created (this
        is ignored if this alias already exists on the parent)

    :param int index:
        insert the new alias into position ``index`` for the ``base_id/ctx``,
        rather than at the end of that list

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        boolean, whether or not the alias was stored. it wouldn't be newly
        stored if the parent object already has an alias with this value.

    :raises ReadOnly: if given a read-only ``pool``

    :raises BadContext:
        if ``ctx`` is not a registered context associated with table.ALIAS, or
        it doesn't have a ``base_ctx`` configured.

    :raises BadFlag:
        if anything in ``flags`` is not a registered flag associated with
        ``ctx``

    :raises AlaisInUse:
        if this ``ctx/value`` pair is already stored under a different
        ``base_id``

    :raises NoObject:
        if there is no parent object for the provided ``base_id`` and the
        ``ctx``'s ``base_ctx``.
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.ALIAS:
        raise error.BadContext(ctx)

    flags = util.flags_to_int(ctx, flags or [])

    return txn.set_alias(pool, base_id, ctx, value, flags, index, timeout)


def lookup(pool, value, ctx, timeout=None):
    '''retrieve an alias record by its value and context

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param str value: the alias value

    :param int ctx: the alias's context

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        an alias dict (containing ``base_id``, ``ctx``, ``value``, and
        ``flags`` keys), or None if there is no alias for the given
        ``ctx/value``
    '''
    result = txn.lookup_alias(pool, hashlib.sha1(value).digest(), ctx, timeout)

    if result is not None:
        # we selected on alias_lookup, which doesn't store the value
        result['value'] = value

    return result


def list(pool, base_id, ctx, limit=100, page_token=None, timeout=None):
    '''list the aliases associated with a guid object for a given context

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the alias's context

    :param int limit: maximum number of aliases to return

    :param str page_token:
        ``page_token`` returned from a previous call to this method. the return
        value will start after the results of that page.

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        two-tuple with a list of alias dicts (containing ``base_id``, ``ctx``,
        ``pos``, ``value``, and ``flags`` keys), and a ``page_token`` that can
        be used in subsequent calls to continue paging.
    '''
    start = -1
    if page_token is not None:
        start = _decode_page_token(page_token)

    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        results, prev_exists = query.select_aliases(
                conn.cursor(), base_id, ctx, limit, start)

    if not prev_exists:
        raise error.BadPageToken(page_token)

    for result in results:
        result['flags'] = util.int_to_flags(ctx, result['flags'])
        pos = result.pop('pos')

    token = None
    if results:
        token = _encode_page_token(pos)

    return results, token


def add_flags(pool, base_id, ctx, value, flags, timeout=None):
    '''apply flags to an existing alias

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the alias's context

    :param str value: string value of the alias

    :param iterable flags: the flags to add

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no alias for the given
        ``base_id/ctx/value``

    :raises ReadOnly: if given a read-only pool

    :raises BadContext:
        if the ``ctx`` is not a registered context for table.ALIAS

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.ALIAS:
        raise error.BadContext(ctx)

    flags = util.flags_to_int(ctx, flags)

    result = txn.add_alias_flags(pool, base_id, ctx, alias, flags, timeout)

    if result is None:
        return None

    return util.int_to_flags(ctx, result)


def clear_flags(pool, base_id, ctx, alias, flags, timeout=None):
    '''remove flags from an existing alias

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the alias's context

    :param str value: string value of the alias

    :param iterable flags: the flags to clear

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no alias for the given
        ``base_id/ctx/value``

    :raises ReadOnly: if given a read-only pool

    :raises BadContext:
        if the ``ctx`` is not a registered context for table.ALIAS

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.ALIAS:
        raise error.BadContext(ctx)

    flags = util.flags_to_int(ctx, flags)

    result = txn.clear_alias_flags(pool, base_id, ctx, alias, flags, timeout)

    if result is None:
        return None

    return util.int_to_flags(ctx, result)


def remove(pool, base_id, ctx, value, timeout=None):
    '''remove a stored alias

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the alias's context

    :param str value: string value of the alias

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        boolean, whether the remove was done or not (it would only fail if
        there is no alias for the given ``base_id/ctx/value``)

    :raises ReadOnly: if given a read-only db connection pool
    '''
    if pool.readonly:
        raise error.ReadOnly()

    return txn.remove_alias(pool, base_id, ctx, alias, timeout)


_map = '0123456789abcdef'
_revmap = {c: i for i, c in enumerate(_map)}

def _encode_page_token(n):
    result = []
    while n:
        result.append(_map[n & 0xf])
        n >>= 4
    return ''.join(result[::-1])

def _decode_page_token(s):
    result = 0
    for c in s:
        result <<= 4
        result |= _revmap[c]
    return result

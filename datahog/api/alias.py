# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

import hashlib
import hmac

from .. import error
from ..const import table, util
from ..db import query, txn


__all__ = ['set', 'lookup', 'list', 'batch', 'add_flags', 'clear_flags',
        'shift', 'remove']


def set(pool, base_id, ctx, value, flags=None, index=None, timeout=None):
    '''set an alias value on a guid object

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the alias's context

    :param unicode value: the alias value

    :param iterable flags:
        the flags to set in the event that the alias is newly created (this
        is ignored if this alias already exists on the parent)

    :param int index:
        insert the new alias into position ``index`` for the ``base_id/ctx``,
        rather than at the end of the list

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

    :raises AliasInUse:
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

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param unicode value: the alias value

    :param int ctx: the alias's context

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        an alias dict (containing ``base_id``, ``ctx``, ``value``, and
        ``flags`` keys), or None if there is no alias for the given
        ``ctx/value``
    '''
    digest = hmac.new(pool.digestkey, value.encode('utf8'),
            hashlib.sha1).digest()
    result = txn.lookup_alias(pool, digest, ctx, timeout)

    if result is not None:
        # we selected on alias_lookup, which doesn't store the value
        result['value'] = value
        result['flags'] = util.int_to_flags(ctx, result['flags'])

    return result


def list(pool, base_id, ctx, limit=100, start=0, timeout=None):
    '''list the aliases associated with a guid object for a given context

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the alias's context

    :param int limit: maximum number of aliases to return

    :param int start:
        an integer representing the index in the list of aliases from which to
        start the results

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        two-tuple with a list of alias dicts (containing ``base_id``, ``ctx``,
        ``value``, and ``flags`` keys), and an integer position that can be
        used as ``start`` in a subsequent call to page forward from after the
        end of this result list.
    '''
    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        results = query.select_aliases(
                conn.cursor(), base_id, ctx, limit, start)

    pos = -1
    for result in results:
        result['flags'] = util.int_to_flags(ctx, result['flags'])
        pos = result.pop('pos')

    return results, pos + 1


def batch(pool, bid_ctx_pairs, timeout=None):
    '''perform a batch lookup of aliases under given base_ids

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param list bid_ctx_pairs:
        a list of two-tuples each containing base_id and ctx. the first alias
        for each base_id/ctx will come up in the results

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a list of the same length as bid_ctx_pairs. if there exists one or more
        alias for each base_id/ctx combination, then the first one (as a dict
        with ``base_id``, ``ctx``, ``flags``, and ``value`` keys) shows up in
        the result list in the same position as the corresponding pair in
        ``bid_ctx_pairs``. if not, then that position is occupied by ``None``.
    '''
    order = {(bid, ctx): i for i, (bid, ctx) in enumerate(bid_ctx_pairs)}
    groups = {}
    for bid, ctx in bid_ctx_pairs:
        groups.setdefault(pool.shard_by_guid(bid), []).append((bid, ctx))

    if timeout is not None:
        deadline = time.time() + timeout

    aliases = []
    for shard, group in groups.iteritems():
        with pool.get_by_shard(shard, timeout=timeout) as conn:
            aliases.extend(query.select_alias_batch(conn.cursor(), group))

        if timeout is not None:
            timeout = deadline - time.time()

    results = [None] * len(bid_ctx_pairs)
    for al in aliases:
        al['flags'] = util.int_to_flags(al['ctx'], al['flags'])
        results[order[(al['base_id'], al['ctx'])]] = al

    return results


def add_flags(pool, base_id, ctx, value, flags, timeout=None):
    '''apply flags to an existing alias

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the alias's context

    :param unicode value: value of the alias

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
    return set_flags(pool, base_id, ctx, value, flags, [], timeout)


def clear_flags(pool, base_id, ctx, value, flags, timeout=None):
    '''remove flags from an existing alias

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the alias's context

    :param unicode value: value of the alias

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
    return set_flags(pool, base_id, ctx, value, [], flags, timeout)


def set_flags(pool, base_id, ctx, value, add, clear, timeout=None):
    '''set and clear flags on an alias

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the alias's context

    :param unicode value: value of the alias

    :param iterable add: the flags to add

    :param iterable clear: the flags to clear

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

    add = util.flags_to_int(ctx, add)
    clear = util.flags_to_int(ctx, clear)

    result = txn.set_alias_flags(
            pool, base_id, ctx, value, add, clear, timeout)

    if result is None:
        return None

    return util.int_to_flags(ctx, result)


def shift(pool, base_id, ctx, value, index, timeout=None):
    '''change the ordered position of an alias

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the alias's context

    :param unicode value: value of the alias

    :param int index: the new index to which to move the alias

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        boolean of whether the move happened or not. it might not happen if
        there is no alias for the given ``base_id/ctx/value``

    :raises ReadOnly: if given a read-only ``pool``
    '''
    if pool.readonly:
        raise error.ReadOnly()

    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        return query.reorder_alias(conn.cursor(), base_id, ctx, value, index)


def remove(pool, base_id, ctx, value, timeout=None):
    '''remove a stored alias

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the alias's context

    :param value: value of the alias

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

    return txn.remove_alias(pool, base_id, ctx, value, timeout)

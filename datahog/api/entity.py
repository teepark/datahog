# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

from .. import error
from ..const import table, util
from ..db import query, txn


__all__ = ['create', 'get', 'batch_get', 'add_flags', 'clear_flags', 'remove']


def create(pool, ctx, flags=None, timeout=None):
    '''store a new entity

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int ctx: the context value for the entity

    :param iterable flags:
        the flags to set on the new entity (default empty)

    :param timeout:
        maximum time in seconds that the method is allowed to take. default of
        ``None`` means no limit

    :returns:
        an entity dict, which contains ``guid``, ``ctx`` and ``flags`` keys

    :raises ReadOnly: if given a read-only pool

    :raises BadContext:
        if the ``ctx`` is not a registered context associated with table.ENTITY

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.ENTITY:
        raise error.BadContext(ctx)

    flags = flags or set()
    flagint = util.flags_to_int(ctx, flags)

    with pool.get_for_entity_write(timeout=timeout) as conn:
        return {
            'guid': query.insert_entity(conn.cursor(), ctx, flagint),
            'ctx': ctx,
            'flags': flags,
        }


def get(pool, guid, ctx, timeout=None):
    '''retrieve a stored entity

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int guid: the guid of the entity to pull

    :param int ctx: the context of the entity

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        an entity dict (which contains ``guid``, ``ctx`` and ``flags`` keys) or
        ``None`` if there exists no entity for the given ``guid/ctx``
    '''
    with pool.get_by_guid(guid, timeout=timeout) as conn:
        ent = query.select_entity(conn.cursor(), guid, ctx)

    if ent is not None:
        ent['flags'] = util.int_to_flags(ctx, ent['flags'])

    return ent


def batch_get(pool, eid_ctx_pairs, timeout=None):
    '''retrieve a list of entities

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param list eid_ctx_pairs:
        list of ``(guid, ctx)`` tuples describing the entities to fetch

    :param timeout:
        maximum time in seconds that the method is allowed to take: the default
        of ``None`` means no limit

    :returns:
        a list of entity dicts containing ``guid``, ``ctx`` and ``flags`` keys.
        for any ``(guid, ctx)`` pairs from ``eid_ctx_pairs`` for which no
        entity could be found, a ``None`` will be in that position in the
        results list.
    '''
    order = {eid: i for i, (eid, ctx) in enumerate(eid_ctx_pairs)}
    groups = {}
    for eid, ctx in eid_ctx_pairs:
        groups.setdefault(pool.shard_by_guid(eid), []).append((eid, ctx))

    if timeout is not None:
        deadline = time.time() + timeout

    ents = []
    for shard, group in groups.iteritems():
        with pool.get_by_shard(shard, timeout=timeout) as conn:
            ents.extend(query.select_entities(conn.cursor(), group))

        if timeout is not None:
            timeout = deadline - time.time()

    results = [None] * len(eid_ctx_pairs)
    for ent in ents:
        ent['flags'] = util.int_to_flags(ent['ctx'], ent['flags'])
        results[order[ent['guid']]] = ent

    return results


def add_flags(pool, guid, ctx, flags, timeout=None):
    '''apply flags to a stored entity

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int guid: the guid of the entity

    :param int ctx: the entity's context

    :param iterable flags: the flags to add

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no entity for the
        given ``guid/ctx``

    :raises ReadOnly: if given a readonly ConnectionPool

    :raises BadContext:
        if the ``ctx`` is not a registered context associated with table.ENTITY

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    return set_flags(pool, guid, ctx, flags, [], timeout)


def clear_flags(pool, guid, ctx, flags, timeout=None):
    '''remove flags from a stored entity

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int guid: the guid of the entity

    :param int ctx: the entity's context

    :param iterable flags: the flags to clear

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no entity for the
        given ``guid/ctx``

    :raises BadContext:
        if the ``ctx`` is not a registered context associated with table.ENTITY

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    return set_flags(pool, guid, ctx, [], flags, timeout)


def set_flags(pool, guid, ctx, add, clear, timeout=None):
    '''set and clear flags on an entity

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int guid: the guid of the entity

    :param int ctx: the entity's context

    :param iterable add: the flags to add

    :param iterable clear: the flags to clear

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no entity for the
        given ``guid/ctx``

    :raises BadContext:
        if the ``ctx`` is not a registered context associated with table.ENTITY

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.ENTITY:
        raise error.BadContext(ctx)

    add = util.flags_to_int(ctx, add)
    clear = util.flags_to_int(ctx, clear)

    with pool.get_by_guid(guid, timeout=timeout) as conn:
        result = query.set_flags(
                conn.cursor(), 'entity', add, clear,
                {'guid': guid, 'ctx': ctx})

    if not result:
        return None

    return util.int_to_flags(ctx, result[0])


def remove(pool, guid, ctx, timeout=None):
    '''remove a stored entity, and all associated objects

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int guid: the guid of the entity

    :param int ctx: the entity's ctx

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        boolean, whether an entity was removed. this would be ``False`` if
        there is no entity for the given ``guid/ctx``

    :raises ReadOnly: if given a read-only pool
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.ENTITY:
        return False

    return txn.remove_entity(pool, guid, ctx, timeout)

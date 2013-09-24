# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

from .. import error, query, txn
from ..const import table, util


__all__ = ['create', 'get', 'add_flags', 'clear_flags', 'remove']


def create(pool, ctx, flags=None, timeout=None):
    '''store a new entity

    :param ConnetionPool pool:
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

    flagint = util.flags_to_int(ctx, flags or [])

    with pool.get_random(timeout=timeout) as conn:
        return {
            'guid': query.insert_entity(conn.cursor(), ctx, flagint),
            'ctx': ctx,
            'flags': flags,
        }


def get(pool, guid, ctx, timeout=None):
    '''retrieve a stored entity

    :param ConnetionPool pool:
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


def add_flags(pool, guid, ctx, flags, timeout=None):
    '''apply flags to a stored entity

    :param ConnetionPool pool:
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
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.ENTITY:
        return None

    flags = util.flags_to_int(ctx, flags)

    with pool.get_by_guid(guid, timeout=timeout) as conn:
        result = query.add_flags(
                conn.cursor(), 'entity', flags, {'guid': guid, 'ctx': ctx})

    if result is None:
        return None

    return util.int_to_flags(ctx, result[0])


def clear_flags(pool, guid, ctx, flags, timeout=None):
    '''remove flags from a stored entity

    :param ConnetionPool pool:
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
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.ENTITY:
        raise error.BadContext(ctx)

    flags = util.flags_to_int(ctx, flags)

    with pool.get_by_guid(guid, timeout=timeout) as conn:
        result = query.clear_flags(
                conn.cursor(), 'entity', flags, {'guid': guid, 'ctx': ctx})

    if result is None:
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

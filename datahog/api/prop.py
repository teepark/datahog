# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

from .. import error
from ..const import context, storage, table, util
from ..db import query, txn


__all__ = ['set', 'get', 'get_list', 'increment', 'set_flags', 'remove']


_missing = object()


def set(pool, base_id, ctx, value, flags=None, timeout=None):
    '''set a property value on a id object

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the id of the parent object

    :param int ctx: the property's context

    :param value:
        the value for the property. depending on the ``ctx``'s configuration,
        this might be different types. see `storage types`_ for more on that.

    :param iterable flags:
        the flags to set in the event that the property is newly created (this
        is ignored if an update is made instead of an insert)

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a two-tuple of ``(inserted, updated)`` bools indicating whether the
        property was created or updated. these will never be both ``True`` or
        ``False`` (in the latter case ``NoObject`` would instead be raised).

    :raises ReadOnly: if given a read-only db pool

    :raises BadContext:
        if ``ctx`` is not a context associated with ``table.PROPERTY``, or it
        doesn't have both a ``base_ctx`` and ``storage`` configured.

    :raises BadFlag:
        if ``flags`` contains something that is not a flag associated with the
        given ``ctx``.

    :raises NoObject:
        if the object specified by ``base_id`` and the configured ``base_ctx``
        doesn't exist.
    '''
    if pool.readonly:
        raise error.ReadOnly()

    base_ctx = util.ctx_base_ctx(ctx)
    if util.ctx_tbl(ctx) != table.PROPERTY or base_ctx is None:
        raise error.BadContext(ctx)

    flags = util.flags_to_int(ctx, flags or [])

    value = util.storage_wrap(ctx, value)

    with pool.get_by_id(base_id, timeout=timeout) as conn:
        inserted, updated = txn.set_property(conn, base_id, ctx, value, flags)

    if not (inserted or updated):
        base_tbl = table.NAMES[util.ctx_tbl(base_ctx)]
        raise error.NoObject("%s<%d/%d>" % (base_tbl, base_ctx, base_id))

    return inserted, updated


def get(pool, base_id, ctx, timeout=None):
    '''retrieve a stored property

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the id of the parent object

    :param int ctx: the property's context

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        property dict (containing ``base_id``, ``ctx``, ``flags``, and
        ``value`` keys) or ``None`` if there is no property for the
        ``base_id/ctx``

    :raises BadContext:
        if ``ctx`` isn't a registered context associated with
        ``table.PROPERTY``, or it doesn't have a configured ``storage``
    '''
    if util.ctx_tbl(ctx) != table.PROPERTY or util.ctx_storage(ctx) is None:
        raise error.BadContext(ctx)

    with pool.get_by_id(base_id, timeout=timeout) as conn:
        exists, value, flags = query.select_property(
                conn.cursor(), base_id, ctx)
        if not exists:
            return None
        return {
            'base_id': base_id,
            'ctx': ctx,
            'flags': util.int_to_flags(ctx, flags),
            'value': util.storage_unwrap(ctx, value),
        }


def get_list(pool, base_id, ctx_list=None, timeout=None):
    '''fetch the properties under a base_id for a list of contexts

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the id of the parent object

    :param ctx_list:
        the contexts of the properties to fetch. can be a list of context ints,
        or ``None`` (default) to fetch all contexts for the ``base_id``

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a list of the same length as ``ctx_list`` of property dicts (containing
        ``base_id``, ``ctx``, ``flags``, and ``value`` keys) or ``None``s,
        depending on whether the property exists for a given context.
    '''
    with pool.get_by_id(base_id, timeout=timeout) as conn:
        results = query.select_properties(conn.cursor(), base_id, ctx_list)

    for r in results:
        if r is not None:
            r['flags'] = util.int_to_flags(r['ctx'], r['flags'])
         
    return results


def increment(pool, base_id, ctx, by=1, limit=None, timeout=None):
    '''increment (or decrement) a numeric property's value

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the id of the parent object

    :param int ctx: the property's context

    :param int by: number to add to the existing property value, default 1

    :param int limit:
        if provided, specifies the maximum (or minimum if ``by < 0``) value
        for the resulting property value (default of ``None`` means no limit)

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the modified integer property value, or None if no property exists for
        the given ``base_id/ctx``.

    :raises ReadOnly: if the provided pool is read-only

    :raises StorageClassError:
        if the ``ctx`` doesn't have a ``storage`` of INT
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_storage(ctx) != storage.INT:
        raise error.StorageClassError(
            'cannot increment a ctx that is not configured for INT')

    with pool.get_by_id(base_id, timeout=timeout) as conn:
        if limit is None:
            return query.increment_property(
                    conn.cursor(), base_id, ctx, by)
        else:
            return query.increment_property(
                    conn.cursor(), base_id, ctx, by, limit)


def set_flags(pool, base_id, ctx, add, clear, timeout=None):
    '''set and/or clear flags on a property

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the id of the parent object

    :param int ctx: the property's context

    :param iterable add: the flags to add

    :param iterable clear: the flags to clear

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no property for the given
        ``base_id/ctx``

    :raises ReadOnly: if given a read-only pool

    :raises BadContext:
        if the ``ctx`` is not a registered context for table.PROPERTY

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.PROPERTY:
        raise error.BadContext(ctx)

    add = util.flags_to_int(ctx, add)
    clear = util.flags_to_int(ctx, clear)

    with pool.get_by_id(base_id, timeout=timeout) as conn:
        result = query.set_flags(
                conn.cursor(), 'property', add, clear,
                {'base_id': base_id, 'ctx': ctx})

    if not result:
        return None

    return util.int_to_flags(ctx, result[0])


def remove(pool, base_id, ctx, value=_missing, timeout=None):
    '''remove a stored property

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the id of the parent object

    :param int ctx: the property's context

    :param value: if provided, will only do the remove if this is the value

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        boolean, whether the remove was done or not (it would only fail if
        there is no property for the given ``base_id/ctx``)

    :raises ReadOnly: if given a read-only db connection pool
    '''
    if pool.readonly:
        raise error.ReadOnly()

    with pool.get_by_id(base_id, timeout=timeout) as conn:
        if value is _missing:
            return query.remove_property(conn.cursor(), base_id, ctx)
        else:
            value = util.storage_wrap(ctx, value)
            return query.remove_property(conn.cursor(), base_id, ctx, value)

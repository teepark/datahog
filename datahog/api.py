# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

import hashlib
import time

import mummy

from . import error, query, txn
from .const import context, table, util


__all__ = ['create_entity', 'get_entity', 'add_entity_flags',
        'clear_entity_flags', 'remove_entity', 'set_property', 'get_property',
        'has_property', 'increment_property', 'add_property_flags',
        'clear_property_flags', 'remove_property', 'set_alias', 'lookup_alias',
        'list_aliases', 'add_alias_flags', 'clear_alias_flags', 'remove_alias',
        'create_relationship', 'list_relationships', 'get_relationship',
        'add_relationship_flags', 'clear_relationship_flags',
        'remove_relationship', 'create_tree_node', 'get_tree_node',
        'list_tree_children', 'get_tree_children', 'update_tree_node',
        'increment_tree_node', 'add_tree_node_flags', 'clear_tree_node_flags',
        'move_tree_node', 'remove_tree_node']


_missing = object()


def create_entity(pool, ctx, flags=None, timeout=None):
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


def get_entity(pool, guid, ctx, timeout=None):
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


def add_entity_flags(pool, guid, ctx, flags, timeout=None):
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


def clear_entity_flags(pool, guid, ctx, flags, timeout=None):
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


def remove_entity(pool, guid, ctx, timeout=None):
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


def set_property(pool, base_id, ctx, value, flags=None, timeout=None):
    '''set a property value on a guid object

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

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

    value = _storage_wrap(ctx, value)

    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        inserted, updated = txn.set_property(conn, base_id, ctx, value, flags)

    if not (inserted or updated):
        base_tbl = table.NAMES[util.ctx_tbl(base_ctx)]
        raise error.NoObject("%s<%d/%d>" % (base_tbl, base_ctx, base_id))

    return inserted, updated


def get_property(pool, base_id, ctx, timeout=None):
    '''retrieve a stored property

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

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

    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        exists, value, flags = query.select_property(
                conn.cursor(), base_id, ctx)
        if not exists:
            return None
        return {
            'base_id': base_id,
            'ctx': ctx,
            'flags': util.int_to_flags(ctx, flags),
            'value': _storage_unwrap(ctx, value),
        }


def has_property(pool, base_id, ctx, timeout=None):
    '''find out whether a given property exists

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the property's context

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns: a bool of whether the property exists
    '''
    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        return query.select_property(conn.cursor(), base_id, ctx)[0]


def increment_property(pool, base_id, ctx, by=1, limit=None, timeout=None):
    '''increment (or decrement) a numeric property's value

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

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
        if the ``ctx`` doesn't have a ``storage`` of STORAGE_INT
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_storage(ctx) != context.STORAGE_INT:
        raise error.StorageClassError(
            'cannot increment a ctx that is not configured for STORAGE_INT')

    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        if limit is None:
            return query.increment_property(
                    conn.cursor(), base_id, ctx, by)
        else:
            return query.increment_property(
                    conn.cursor(), base_id, ctx, by, limit)


def add_property_flags(pool, base_id, ctx, flags, timeout=None):
    '''apply flags to an existing property

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the property's context

    :param iterable flags: the flags to add

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

    flags = util.flags_to_int(ctx, flags)

    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        result = query.add_flags(conn.cursor(),
                'property', flags, {'base_id': base_id, 'ctx': ctx})

    if result is None:
        return None

    return util.int_to_flags(ctx, result[0])


def clear_property_flags(pool, base_id, ctx, flags, timeout=None):
    '''remove flags from an existing property

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the property's context

    :param iterable flags: the flags to clear

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

    flags = util.flags_to_int(ctx, flags)

    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        result = query.clear_flags(conn.cursor(),
                'property', flags, {'base_id': base_id, 'ctx': ctx})

    if result is None:
        return None

    return util.int_to_flags(ctx, result[0])


def remove_property(pool, base_id, ctx, value=_missing, timeout=None):
    '''remove a stored property

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

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

    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        if value is _missing:
            return query.remove_property(conn.cursor(), base_id, ctx)
        else:
            value = _storage_wrap(ctx, value)
            return query.remove_property(conn.cursor(), base_id, ctx, value)


def set_alias(pool, base_id, ctx, value, flags=None, timeout=None):
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

    return txn.set_alias(pool, base_id, ctx, value, flags, timeout)


def lookup_alias(pool, value, ctx, timeout=None):
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


def list_aliases(pool, base_id, ctx, timeout=None):
    '''list the aliases associated with a guid object for a given context

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the alias's context

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a list of alias dicts (containing ``base_id``, ``ctx``, ``value``, and
        ``flags`` keys)
    '''
    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        results = query.select_aliases(conn.cursor(), base_id, ctx)

    for result in results:
        result['flags'] = util.int_to_flags(ctx, result['flags'])

    return results


def add_alias_flags(pool, base_id, ctx, value, flags, timeout=None):
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


def clear_alias_flags(pool, base_id, ctx, alias, flags, timeout=None):
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


def remove_alias(pool, base_id, ctx, value, timeout=None):
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


def create_relationship(pool, ctx, base_id, rel_id, flags=None, timeout=None):
    '''make a new relationship between two guid objects

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int ctx: the context for the relationship

    :param int base_id: the guid of the first related object

    :param int rel_id: the guid of the other related object

    :param iterable flags:
        the flags to set on the new relationship (default empty). these will be
        ignored if the relationship already exists (therefore isn't newly
        created by this method)

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a boolean of whether the new relationship was created. it wouldn't be
        created in the case that a relationship with the same
        ``ctx/base_id/rel_id`` already exists.

    :raises ReadOnly: if given a read-only db connection pool

    :raises BadContext:
        if ``ctx`` is not a context associated with ``table.RELATIONSHIP``, or
        it doesn't have both a ``base_ctx`` and a ``rel_ctx`` configured.

    :raises BadFlag:
        if ``flags`` contains something that is not a flag associated with the
        given ``ctx``

    :raises NoObject:
        if either of the objects at ``base_ctx/base_id`` or ``rel_ctx/rel_id``
        don't exist
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if (util.ctx_tbl(ctx) != table.RELATIONSHIP
            or util.ctx_base_ctx(ctx) is None
            or util.ctx_rel_ctx(ctx) is None):
        raise error.BadContext(ctx)

    flags = util.flags_to_int(ctx, flags or [])

    return txn.create_relationship_pair(
            pool, base_id, rel_id, ctx, flags, timeout)


def list_relationships(pool, guid, ctx, forward=True, timeout=None):
    '''list the relationships associated with a guid object

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int guid: guid of the parent object

    :param int ctx: context of the relationships to fetch

    :param bool forward:
        if ``True``, then fetches relationships which have ``guid`` as their
        ``base_id``, otherwise ``guid`` refers to ``rel_id``

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a list of relationship dicts, which contain ``ctx``, ``base_id``,
        ``rel_id``, and ``flags`` keys
    '''
    with pool.get_by_guid(guid, timeout=timeout) as conn:
        results = query.select_relationships(conn.cursor(), guid, ctx, forward)

    for result in results:
        result['flags'] = util.int_to_flags(ctx, result['flags'])

    return results


def get_relationship(pool, ctx, base_id, rel_id, timeout=None):
    '''fetch the relationship between two guids

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int ctx: context of the relationship

    :param int base_id: guid of the object at one end

    :param int rel_id: guid of the object at the other end

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a relationship dict (with ``ctx``, ``base_id``, ``rel_id``, and
        ``flags`` keys) or None if there is no such relationship
    '''
    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        rels = query.select_relationships(
                conn.cursor(), base_id, ctx, True, rel_id)
    return rels[0] if rels else None


def add_relationship_flags(pool, base_id, rel_id, ctx, flags, timeout=None):
    '''apply flags to an existing relationship

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the object at one end

    :param int rel_id: the guid of the object at the other end

    :param int ctx: the relationship's context

    :param iterable flags: the flags to add

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no relationship for the given
        ``base_id/rel_id/ctx``

    :raises ReadOnly: if given a read-only pool

    :raises BadContext:
        if the ``ctx`` is not a registered context for table.RELATIONSHIP

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.RELATIONSHIP:
        raise error.BadContext(ctx)

    flags = util.flags_to_int(ctx, flags)

    result = txn.add_relationship_flags(
            pool, base_id, rel_id, ctx, flags, timeout)

    if result is None:
        return None

    return util.int_to_flags(ctx, result)


def clear_relationship_flags(pool, base_id, rel_id, ctx, flags, timeout=None):
    '''remove flags from a relationship

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the object at one end

    :param int rel_id: the guid of the object at the other end

    :param int ctx: the relationship's context

    :param iterable flags: the flags to add

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no relationship for the given
        ``base_id/rel_id/ctx``

    :raises ReadOnly: if given a read-only pool

    :raises BadContext:
        if the ``ctx`` is not a registered context for table.RELATIONSHIP

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.RELATIONSHIP:
        raise error.BadContext(ctx)

    flags = util.flags_to_int(ctx, flags)

    result = txn.clear_relationship_flags(
            pool, base_id, rel_id, ctx, flags, timeout)

    if result is None:
        return None

    return util.int_to_flags(ctx, result)


def remove_relationship(pool, base_id, rel_id, ctx, timeout=None):
    '''remove a relationship

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the object at one end

    :param int rel_id: the guid of the object at the other end

    :param int ctx: the relationshp's context

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

    return txn.remove_relationship_pair(pool, base_id, rel_id, ctx, timeout)


def create_tree_node(pool, base_id, ctx, value, flags=None, timeout=None):
    '''make a new tree node

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the node's context

    :param value:
        the value for the node. depending on the ``ctx``'s configuration,
        this might be different types. see `storage types`_ for more on that.

    :param iterable flags: any flags to set on the new node

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a tree_node dict, containing ``guid``, ``ctx``, ``value``, and
        ``flags`` keys

    :raises ReadOnly: if the provided pool is read-only

    :raises BadContext:
        if ``ctx`` is not a context associated with table.TREENODE, or doesn't
        have both ``base_ctx`` and ``storage`` configured

    :raises StorageClassError:
        if ``value`` doesn't have the right type for the configured ``storage``

    :raises NoObject:
        if the parent object at ``base_ctx/base_id`` doesn't exist
    '''
    if pool.readonly:
        raise error.ReadOnly()

    base_ctx = util.ctx_base_ctx(ctx)
    if util.ctx_tbl(ctx) != table.TREENODE or base_ctx is None:
        raise error.BadContext(ctx)

    flags = flags or []
    flagsint = util.flags_to_int(ctx, flags)
    value = _storage_wrap(ctx, value)

    node = txn.create_tree_node(pool, base_id, ctx, value, flagsint, timeout)

    if node is None:
        base_tbl = table.NAMES[util.ctx_tbl(base_ctx)]
        raise error.NoObject("%s<%d/%d>" % (base_tbl, base_ctx, base_id))

    node['flags'] = flags

    return node


def get_tree_node(pool, node_id, ctx, timeout=None):
    '''fetch an existing tree node

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int node_id: the guid of the node to fetch

    :param int ctx: the node's context

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a node dict (contains ``guid``, ``ctx``, ``value``, and ``flags``
        keys), or ``None`` if there is no such tree node

    :raises BadContext:
        if ``ctx`` isn't a registered context for ``table.TREENODE``, or
        doesn't have both a ``base_ctx`` and ``storage`` configured
    '''
    if (util.ctx_tbl(ctx) != table.TREENODE
            or util.ctx_base_ctx(ctx) is None
            or util.ctx_storage(ctx) is None):
        raise error.BadContext(ctx)

    with pool.get_by_guid(node_id, timeout=timeout) as conn:
        node = query.select_tree_node(conn.cursor(), node_id, ctx)

    if node is None:
        return None

    node['flags'] = util.int_to_flags(ctx, node['flags'])
    node['value'] = _storage_unwrap(ctx, node['value'])

    return node


def get_tree_nodes(pool, nid_ctx_pairs, timeout=None):
    '''fetch a list of tree nodes

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param list nid_ctx_pairs:
        list of ``(guid, ctx)`` tuples describing the nodes to fetch

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a list of node dicts containing ``guid``, ``ctx``, ``value`` and
        ``flags`` keys. any ``(guid, ctx)`` pairs from ``nid_ctx_pairs`` for
        which no node could be found, a None will be in that position in the
        results list
    '''
    order = {nid: i for i, (nid, ctx) in enumerate(nid_ctx_pairs)}
    groups = {}
    for nid, ctx in nid_ctx_pairs:
        groups.setdefault(pool.shard_by_guid(nid), []).append((nid, ctx))

    if timeout is not None:
        deadline = time.time() + timeout

    nodes = []
    for shard, group in groups.iteritems():
        with pool.get_by_shard(shard, timeout=timeout) as conn:
            nodes.extend(
                    query.select_tree_nodes(conn.cursor(), group))

        if timeout is not None:
            timeout = deadline - time.time()

    results = [None] * len(nid_ctx_pairs)
    for node in nodes:
        node['flags'] = util.int_to_flags(node['ctx'], node['flags'])
        node['value'] = _storage_unwrap(node['ctx'], node['value'])
        results[order[node['guid']]] = node

    return results


def list_tree_children(pool, base_id, ctx, timeout=None):
    '''list the tree nodes' guids under a common parent

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: context of the nodes

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a list of ints of the guids of the tree nodes

    :raises BadContext:
        if ``ctx`` isn't a registered context for ``table.TREENODE``, or
        doesn't have both a ``base_ctx`` and ``storage`` configured
    '''
    if (util.ctx_tbl(ctx) != table.TREENODE
            or util.ctx_base_ctx(ctx) is None
            or util.ctx_storage(ctx) is None):
        raise error.BadContext(ctx)

    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        results = query.select_tree_node_guids(conn.cursor(), base_id, ctx)

    return [pair[0] for pair in results]


def get_tree_children(pool, base_id, ctx, timeout=None):
    '''fetch the tree nodes under a common parent

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: context of the nodes

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a list of tree node dicts, each containing ``guid``, ``ctx``, ``value``
        and ``flags`` keys

    :raises BadContext:
        if ``ctx`` isn't a registered context for ``table.TREENODE``, or
        doesn't have both a ``base_ctx`` and ``storage`` configured
    '''
    if timeout is not None:
        deadline = time.time() + timeout

    nids = list_tree_children(pool, base_id, ctx, timeout)

    if timeout is not None:
        timeout = deadline - time.time()

    nodes = get_tree_nodes(pool, [(nid, ctx) for nid in nids], timeout)

    return [node for node in nodes if node is not None]


def update_tree_node(
        pool, node_id, ctx, value, old_value=_missing, timeout=None):
    '''overwrite the value stored in a tree_node

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int node_id: guid of the node

    :param int ctx: the node's context

    :param value: the new value to set on the node

    :param old_value:
        if provided, only do the update if this is the current value

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a bool of whether the update happened. reasons that it might not are
        that the node doesn't exist at all, or ``old_value`` was provided but
        the node has a different value

    :raises ReadOnly: if given a read-only pool

    :raises BadContext:
        if ``ctx`` isn't a registered context for ``table.TREENODE``, or
        doesn't have both a ``base_ctx`` and ``storage`` configured
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if (util.ctx_tbl(ctx) != table.TREENODE
            or util.ctx_base_ctx(ctx) is None
            or util.ctx_storage(ctx) is None):
        raise error.BadContext(ctx)

    value = _storage_wrap(ctx, value)

    with pool.get_by_guid(node_id, timeout=timeout) as conn:
        if old_value is _missing:
            return query.update_tree_node(conn.cursor(), node_id, ctx, value)
        else:
            old_value = _storage_wrap(ctx, old_value)
            return query.update_tree_node(
                    conn.cursor(), node_id, ctx, value, old_value)


def increment_tree_node(pool, node_id, ctx, by=1, limit=None, timeout=None):
    '''increment (or decrement) a numeric tree node's value

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int node_id: the guid of the parent object

    :param int ctx: the node's context

    :param int by: number to add to the existing property value, default 1

    :param int limit:
        if provided, specifies the maximum (or minimum if ``by < 0``) value
        for the resulting value (default of ``None`` means no limit)

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the modified integer node value, or None if no property exists for the
        given ``node_id/ctx``.

    :raises ReadOnly: if the provided pool is read-only

    :raises StorageClassError:
        if the ``ctx`` doesn't have a ``storage`` of STORAGE_INT
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_storage(ctx) != context.STORAGE_INT:
        raise error.StorageClassError(
            'cannot increment a ctx that is not configured for STORAGE_INT')

    with pool.get_by_guid(node_id, timeout=timeout) as conn:
        if limit is None:
            return query.increment_tree_node(conn.cursor(), node_id, ctx, by)
        else:
            return query.increment_tree_node(
                    conn.cursor(), node_id, ctx, by, limit)


def add_tree_node_flags(pool, node_id, ctx, flags, timeout=None):
    '''apply flags to a stored tree node

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int node_id: the guid of the node

    :param int ctx: the node's context

    :param iterable flags: the flags to add

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no node for the 
        given ``node_id/ctx``

    :raises ReadOnly: if given a read-only ConnectionPool

    :raises BadContext:
        if the ``ctx`` is not a registered context associated with
        table.TREENODE

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.TREENODE:
        raise error.BadContext(ctx)

    flags = util.flags_to_int(ctx, flags)

    with pool.get_by_guid(node_id, timeout=timeout) as conn:
        result = query.add_flags(conn.cursor(), 'tree_node', flags,
                {'guid': node_id, 'ctx': ctx})

    if result is None:
        return None

    return util.int_to_flags(ctx, result[0])


def clear_tree_node_flags(pool, node_id, ctx, flags, timeout=None):
    '''remove flags from a stored tree node

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int node_id: the guid of the node

    :param int ctx: the node's context

    :param iterable flags: the flags to clear

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no node for the 
        given ``node_id/ctx``

    :raises ReadOnly: if given a read-only ConnectionPool

    :raises BadContext:
        if the ``ctx`` is not a registered context associated with
        table.TREENODE

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.TREENODE:
        raise error.BadContext(ctx)

    flags = util.flags_to_int(flags)

    with pool.get_by_guid(node_id, timeout=timeout) as conn:
        result = query.clear_flags(conn.cursor(), 'tree_node', flags,
                {'guid': node_id, 'ctx': ctx})

    if result is None:
        return None

    return util.int_to_flags(ctx, result[0])


def move_tree_node(pool, node_id, ctx, base_id, new_base_id, timeout=None):
    '''
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.TREENODE:
        raise error.BadContext(ctx)

    return txn.move_tree_node(
            pool, node_id, ctx, base_id, new_base_id, timeout)


def remove_tree_node(pool, node_id, ctx, base_id, timeout=None):
    '''remove a tree node and all associated objects

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int node_id: the guid of the node

    :param int ctx: the node's ctx

    :param int base_id: the guid of the node's parent object

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        boolean, whether a node was removed. this would be ``False`` if there
        is no node for the given ``node_id/ctx/base_id``

    :raises ReadOnly: if given a read-only pool
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.TREENODE:
        return False

    return txn.remove_tree_node(pool, node_id, ctx, base_id, timeout)


def _storage_wrap(ctx, value):
    st = util.ctx_storage(ctx)

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



def _storage_unwrap(ctx, value):
    st = util.ctx_storage(ctx)
    if st is None:
        raise error.BadContext(ctx)

    if st == context.STORAGE_UTF8:
        return st.decode("utf8")

    if st == context.STORAGE_SER:
        return mummy.loads(value)

    return value

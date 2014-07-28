# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

import time

from .. import error
from ..const import context, storage, table, util
from ..db import query, txn


__all__ = ['create', 'get', 'batch_get', 'child_of', 'list_children',
        'get_children', 'update', 'increment', 'add_flags', 'clear_flags',
        'set_flags', 'move', 'shift', 'remove']


_missing = object()


def create(pool, base_id, ctx, value, index=None, flags=None, timeout=None):
    '''make a new node

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the node's context

    :param value:
        the value for the node. depending on the ``ctx``'s configuration,
        this might be different types. see `storage types`_ for more on that.

    :param int index:
        insert the new node into position ``index`` for the ``base_id/ctx``,
        rather than at the end of the list

    :param iterable flags: any flags to set on the new node

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a node dict, containing keys ``guid``, ``ctx``, ``value``, ``flags``

    :raises ReadOnly: if the provided pool is read-only

    :raises BadContext:
        if ``ctx`` is not a context associated with table.NODE, or doesn't
        have both ``base_ctx`` and ``storage`` configured

    :raises StorageClassError:
        if ``value`` doesn't have the right type for the configured ``storage``

    :raises NoObject:
        if the parent object at ``base_ctx/base_id`` doesn't exist
    '''
    if pool.readonly:
        raise error.ReadOnly()

    base_ctx = util.ctx_base_ctx(ctx)
    if util.ctx_tbl(ctx) != table.NODE or base_ctx is None:
        raise error.BadContext(ctx)

    flags = util.flags_to_int(ctx, flags or [])
    value = util.storage_wrap(ctx, value)

    node = txn.create_node(pool, base_id, ctx, value, index, flags, timeout)

    if node is None:
        base_tbl = table.NAMES[util.ctx_tbl(base_ctx)]
        raise error.NoObject("%s<%d/%d>" % (base_tbl, base_ctx, base_id))

    node['flags'] = util.int_to_flags(ctx, node['flags'])
    node['value'] = util.storage_unwrap(ctx, node['value'])

    return node


def get(pool, node_id, ctx, timeout=None):
    '''fetch an existing node

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
        keys), or ``None`` if there is no such node

    :raises BadContext:
        if ``ctx`` isn't a registered context for ``table.NODE``, or
        doesn't have both a ``base_ctx`` and ``storage`` configured
    '''
    if (util.ctx_tbl(ctx) != table.NODE
            or util.ctx_base_ctx(ctx) is None
            or util.ctx_storage(ctx) is None):
        raise error.BadContext(ctx)

    with pool.get_by_guid(node_id, timeout=timeout) as conn:
        node = query.select_node(conn.cursor(), node_id, ctx)

    if node is None:
        return None

    node['flags'] = util.int_to_flags(ctx, node['flags'])
    node['value'] = util.storage_unwrap(ctx, node['value'])

    return node


def batch_get(pool, nid_ctx_pairs, timeout=None):
    '''fetch a list of nodes

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
                    query.select_nodes(conn.cursor(), group))

        if timeout is not None:
            timeout = deadline - time.time()

    results = [None] * len(nid_ctx_pairs)
    for node in nodes:
        node['flags'] = util.int_to_flags(node['ctx'], node['flags'])
        node['value'] = util.storage_unwrap(node['ctx'], node['value'])
        results[order[node['guid']]] = node

    return results


def child_of(pool, node_id, ctx, base_id, timeout=None):
    '''determine whether a node's parent is a particular base_id

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int node_id: the child node's guid

    :param int ctx: the child node's context

    :param int base_id: the parent guid

    :param timeout:
        maximum time in seconds to allow the method to block. default of
        ``None`` means no limit.

    :returns: boolean of whether the child exists

    :raises BadContext:
        if ``ctx`` isn't registered for ``table.NODE``, or doesn't have both
        a ``base_ctx`` and ``storage`` configured
    '''
    if (util.ctx_tbl(ctx) != table.NODE
            or util.ctx_base_ctx(ctx) is None
            or util.ctx_storage(ctx) is None):
        raise error.BadContext(ctx)

    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        return query.select_edge_exists(
                conn.cursor(), node_id, ctx, base_id)


def list_children(pool, base_id, ctx, limit=100, start=0, timeout=None):
    '''list the nodes' guids under a common parent

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: context of the nodes

    :param int limit: maximum number of nodes to return

    :param int start:
        an integer representing the index in the list of nodes from which to
        start the results

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        two tuple with a list of ints of the guids of the nodes, and an integer
        that can be used as ``start`` in subsequent ``list_children`` calls to
        pick up paging after this result list

    :raises BadContext:
        if ``ctx`` isn't a registered context for ``table.NODE``, or
        doesn't have both a ``base_ctx`` and ``storage`` configured
    '''
    if (util.ctx_tbl(ctx) != table.NODE
            or util.ctx_base_ctx(ctx) is None
            or util.ctx_storage(ctx) is None):
        raise error.BadContext(ctx)

    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        results = query.select_node_guids(
                conn.cursor(), base_id, limit, start, ctx)

    end = results[-1][2] + 1 if results else 0

    return [group[0] for group in results], end


def get_children(pool, base_id, ctx, limit=100, start=0, timeout=None):
    '''fetch the nodes under a common parent

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: context of the nodes

    :param int limit: maximum number of nodes to return

    :param int start:
        an integer representing the index in the list of nodes from which to
        start the results

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        two tuple with a list of node dicts (each containing ``guid``, ``ctx``,
        ``value`` and ``flags`` keys), and an integer that can be used as
        ``start`` in subsequent ``get_children`` calls to pick up paging after
        this result list

    :raises BadContext:
        if ``ctx`` isn't a registered context for ``table.NODE``, or
        doesn't have both a ``base_ctx`` and ``storage`` configured
    '''
    if timeout is not None:
        deadline = time.time() + timeout

    nids, pos = list_children(pool, base_id, ctx, limit, start, timeout)

    if timeout is not None:
        timeout = deadline - time.time()

    nodes = batch_get(pool, [(nid, ctx) for nid in nids], timeout)

    return [node for node in nodes if node is not None], pos


def update(pool, node_id, ctx, value, old_value=_missing, timeout=None):
    '''overwrite the value stored in a node

    :param ConnectionPool pool:
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
        if ``ctx`` isn't a registered context for ``table.NODE``, or
        doesn't have both a ``base_ctx`` and ``storage`` configured
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if (util.ctx_tbl(ctx) != table.NODE
            or util.ctx_base_ctx(ctx) is None
            or util.ctx_storage(ctx) is None):
        raise error.BadContext(ctx)

    value = util.storage_wrap(ctx, value)

    with pool.get_by_guid(node_id, timeout=timeout) as conn:
        if old_value is _missing:
            return query.update_node(conn.cursor(), node_id, ctx, value)
        else:
            old_value = util.storage_wrap(ctx, old_value)
            return query.update_node(
                    conn.cursor(), node_id, ctx, value, old_value)


def increment(pool, node_id, ctx, by=1, limit=None, timeout=None):
    '''increment (or decrement) a numeric node's value

    :param ConnectionPool pool:
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
        if the ``ctx`` doesn't have a ``storage`` of INT
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_storage(ctx) != storage.INT:
        raise error.StorageClassError(
            'cannot increment a ctx that is not configured for INT')

    with pool.get_by_guid(node_id, timeout=timeout) as conn:
        if limit is None:
            return query.increment_node(conn.cursor(), node_id, ctx, by)
        else:
            return query.increment_node(
                    conn.cursor(), node_id, ctx, by, limit)


def add_flags(pool, node_id, ctx, flags, timeout=None):
    '''apply flags to a stored node

    :param ConnectionPool pool:
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
        table.NODE

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    return set_flags(pool, node_id, ctx, flags, [], timeout)


def clear_flags(pool, node_id, ctx, flags, timeout=None):
    '''remove flags from a stored node

    :param ConnectionPool pool:
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
        table.NODE

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    return set_flags(pool, node_id, ctx, [], flags, timeout)


def set_flags(pool, node_id, ctx, add, clear, timeout=None):
    '''set and clear flags on a node

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int node_id: the guid of the node

    :param int ctx: the node's context

    :param iterable add: the flags to add

    :param iterable clear: the flags to clear

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no node for the given
        ``guid/ctx``

    :raises BadContext:
        if the ``ctx`` is not a registered context associated with table.ENTITY

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    if pool.readonly:
        return error.ReadOnly()

    if util.ctx_tbl(ctx) != table.NODE:
        raise error.BadContext(ctx)

    add = util.flags_to_int(ctx, add)
    clear = util.flags_to_int(ctx, clear)

    with pool.get_by_guid(node_id, timeout=timeout) as conn:
        result = query.set_flags(conn.cursor(), 'node', add, clear,
                {'guid': node_id, 'ctx': ctx})

    if not result:
        return None

    return util.int_to_flags(ctx, result[0])


def shift(pool, node_id, ctx, base_id, index, timeout=None):
    '''change the ordered position of a node among its siblings

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int node_id: the guid of the node

    :param int ctx: the node's ctx

    :param int base_id: the guid of the node's parent object

    :param int index: the 0-indexed position to which to move the node

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        boolean, whether or not the change was applied. it might return
        ``False`` if there is no node for the given ``node_id/ctx/base_id``

    :raises ReadOnly: if given a read-only pool
    '''
    if pool.readonly:
        raise error.ReadOnly()

    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        return query.reorder_edge(conn.cursor(), base_id, ctx, node_id, index)


def move(pool, node_id, ctx, base_id, new_base_id, index=None, timeout=None):
    '''move a node to underneath a new parent object

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int node_id: the guid of the node

    :param int ctx: the node's ctx

    :param int base_id: the guid of the node's current parent object

    :param int new_base_id: the guid of the target parent

    :param int index:
        the position in the nodes under ``new_base_id/ctx`` into which to
        insert this node, instead of at the end of the list

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        boolean, whether a node was moved. this would be ``False`` if there
        is no node for the given ``node_id/ctx/base_id`` or if the target
        parent object doesn't exist

    :raises ReadOnly: if given a read-only pool

    :raises BadContext:
        if ``ctx`` doesn't correspond to a ``table.NODE`` context
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.NODE:
        raise error.BadContext(ctx)

    return txn.move_node(
            pool, node_id, ctx, base_id, new_base_id, index, timeout)


def remove(pool, node_id, ctx, base_id, timeout=None):
    '''remove a node and all associated objects

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

    if util.ctx_tbl(ctx) != table.NODE:
        return False

    return txn.remove_node(pool, node_id, ctx, base_id, timeout)

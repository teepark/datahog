# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

from .. import error
from ..const import search as searchconst, table, util
from ..db import query, txn


def create(pool, base_id, ctx, value, flags=None, index=None, timeout=None):
    '''store a name on a guid object

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the name's context

    :param str value: the name text

    :param iterable flags: the flags to set on the new name

    :param int index:
        insert the new name into this zero-indexed position in the list of
        names for the given ``base_id/ctx`` (the default of ``None`` attaches
        it onto the end).

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        boolean of whether or not the name was stored. it might come back
        ``False`` if no object exists for the given ``base_id`` and the
        ``base_ctx`` of the provided ``ctx``

    :raises ReadOnly: if given a read-only ``pool``

    :raises BadContext:
        if ``ctx`` is not a valid context configured for ``table.NAME``

    :raises BadFlag:
        if anything in ``flags`` is not a registered flag associated with
        ``ctx``
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.NAME:
        raise error.BadContext(ctx)

    flags = util.flags_to_int(ctx, flags or [])

    return txn.create_name(pool, base_id, ctx, value, flags, index, timeout)


def search(pool, value, ctx, limit=100, start=None, timeout=None):
    '''collect the names matching a search query for a given context

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param str value:
        the search query (usage of this depends on the configured search class
        for ``ctx``)

    :param int ctx: name context to search under

    :param int limit: maximum number of names to return

    :param start:
        a page_token object returned from a previous call to ``search``, which
        can be used to pick up paging from where that search left off

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a two-tuple with a list of name dicts (each containing ``base_id``,
        ``ctx``, ``value``, and ``flags`` keys), and a ``page_token`` that can
        be used as the value of ``start`` in subsequent calls to continue
        paging from the end of this result list. the result list will be in
        sorted order of the string name values.
    '''
    if util.ctx_search(ctx) is None:
        raise error.BadContext(ctx)

    results = txn.search_names(pool, value, ctx, limit, start, timeout)

    for result in results:
        result['flags'] = util.int_to_flags(ctx, result['flags'])

    return results, _token_for_searchlist(ctx, results)


def list(pool, base_id, ctx, limit=100, start=0, timeout=None):
    '''list the names under a guid object for a given context

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: guid of the parent object to search under

    :param int ctx: context of the names to list

    :param int limit: maximum number of names to return

    :param int start:
        an integer representing the index in the list of aliases from which
        to start the results

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a two-tuple with a list of name dicts (each containing ``base_id``,
        ``ctx``, ``value``, and ``flags`` keys) and a ``page_token`` that can
        be used as the value of ``start`` in subsequent calls, to continue
        paging from the end of this result list
    '''
    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        results = query.select_names(conn.cursor(), base_id, ctx, limit, start)

    pos = -1
    for result in results:
        result['flags'] = util.int_to_flags(ctx, result['flags'])
        pos = result.pop('pos')

    return results, pos + 1


def add_flags(pool, base_id, ctx, value, flags, timeout=None):
    '''apply flags to an existing name

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the name's context

    :param str value: the name value

    :param iterable flags: the flags to add

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no alias for the given
        ``base_id/ctx/value``

    :raises ReadOnly: if given a read-only pool

    :raises BadContext:
        if the ``ctx`` is not a registered context for table.NAME

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''


def clear_flags(pool, base_id, ctx, value, flags, timeout=None):
    '''remove flags from an existing name

    :param ConnetionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the name's context

    :param str value: the name value

    :param iterable flags: the flags to clear

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no alias for the given
        ``base_id/ctx/value``

    :raises ReadOnly: if given a read-only pool

    :raises BadContext:
        if the ``ctx`` is not a registered context for table.NAME

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''


def shift(pool, base_id, ctx, value, index, timeout=None):
    '''change the ordered position of a name

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the name's context

    :param str value: name string

    :param int index: the new zero-index position to which to move the name

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        boolean of whether the move happened or not. it might not happen if
        there is not name for the given ``base_id/ctx/value``

    :raises ReadOnly: if given a read-only ``pool``
    '''


def remove(pool, base_id, ctx, value, timeout=None):
    '''remove a stored name

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the guid of the parent object

    :param int ctx: the name's context

    :param str value: string value of the name

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        boolean, whether the remove was done or not (it would only fail if
        there is no name for the given ``base_id/ctx/value``)

    :raises ReadOnly: if given a read-only ``pool``
    '''


def _token_for_searchlist(ctx, results):
    if not results:
        return None

    sclass = util.ctx_search(ctx)

    if sclass == searchconst.PREFIX:
        return results[-1]['value']

    if sclass is None:
        raise error.BadContext(ctx)

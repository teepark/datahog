# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

import contextlib
import hashlib
import random
import sys
import time

import psycopg2
import psycopg2.extensions

from . import query
from .. import error
from ..const import search, table, util


class TwoPhaseCommit(object):
    def __init__(self, pool, shard, name, uniq_data):
        self._pool = pool
        self._shard = shard
        self._name = name
        self._uniq_data = uniq_data
        self._conn = None
        self._failed = False

    def _free_conn(self):
        self._pool.put(self._conn)
        self._conn = None

    def _get_conn(self):
        if self._conn is None:
            self._conn = self._pool.get_by_shard(
                    self._shard, replace=False)

        return self._conn

    def rollback(self):
        conn = self._get_conn()
        try:
            conn.tpc_rollback(self._xid)

        except Exception:
            conn.reset()
            raise

        finally:
            self._free_conn()

    def commit(self):
        conn = self._get_conn()
        try:
            conn.tpc_commit(self._xid)

        except Exception:
            conn.reset()
            raise

        finally:
            self._free_conn()

    def fail(self):
        self._failed = True

    def __enter__(self):
        intxn = False
        conn = self._get_conn()

        xid = conn.xid(random.randrange(1<<31), self._name,
                '-'.join(map(str, self._uniq_data)))
        self._xid = xid
        conn.tpc_begin(xid)

        return conn

    def __exit__(self, klass=None, exc=None, tb=None):
        try:
            if self._failed or exc is not None:
                self._conn.tpc_rollback()
                self._failed = True
            else:
                self._conn.tpc_prepare()
                self._conn.reset()

        finally:
            self._conn = None

    @contextlib.contextmanager
    def elsewhere(self):
        if self._failed:
            raise RuntimeError("TPC already failed")

        try:
            yield

        except Exception:
            exc, klass, tb = sys.exc_info()
            try:
                self.rollback()
            except Exception:
                pass

            raise exc, klass, tb

        else:
            if self._failed:
                self.rollback()
            else:
                self.commit()


class Timer(object):
    def __init__(self, pool, timeout, conn):
        self.pool = pool
        self.timeout = timeout
        self.conn = conn

    def __enter__(self):
        self.t = self.pool._timer(self.timeout, self.ding)
        self.t.start()
        return self

    def __exit__(self, klass=None, exc=None, tb=None):
        if klass is psycopg2.extensions.QueryCanceledError:
            raise error.Timeout()
        self.t.cancel()

    def ding(self):
        if self.conn is not None:
            self.conn.cancel()


def set_property(conn, base_id, ctx, value, flags):
    cursor = conn.cursor()
    try:
        result = query.upsert_property(cursor, base_id, ctx, value, flags)
        conn.commit()
        return result

    except psycopg2.IntegrityError:
        conn.rollback()
        updated = query.update_property(cursor, base_id, ctx, value, flags)
        conn.commit()
        return False, bool(updated)


def lookup_alias(pool, digest, ctx, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _lookup_alias(pool, digest, ctx, timer)
    with timer:
        return _lookup_alias(pool, digest, ctx, timer)

def _lookup_alias(pool, digest, ctx, timer):
    for shard in pool.shards_for_lookup_hash(digest):
        conn = pool.get_by_shard(shard, replace=False)
        timer.conn = conn
        try:
            alias = query.select_alias_lookup(conn.cursor(), digest, ctx)
            if alias is not None:
                return alias

        finally:
            conn.rollback()
            pool.put(conn)
            timer.conn = None

    return None


def set_alias(pool, base_id, ctx, alias, flags, index, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _set_alias(pool, base_id, ctx, alias, flags, index, timer)
    with timer:
        return _set_alias(pool, base_id, ctx, alias, flags, index, timer)

def _set_alias(pool, base_id, ctx, alias, flags, index, timer):
    digest = hashlib.sha1(alias).digest()
    digest_b64 = digest.encode('base64').strip()

    # look up pre-existing aliases on any but the current insert shard
    insert_shard = pool.shard_for_alias_write(digest)
    owner = None
    for shard in pool.shards_for_lookup_hash(digest):
        if shard == insert_shard:
            continue

        with pool.get_by_shard(shard) as conn:
            timer.conn = conn
            try:
                owner = query.select_alias_lookup(conn.cursor(), digest, ctx)
            finally:
                timer.conn = None
        del conn

        if owner is not None:
            break

    if owner is not None:
        if owner['base_id'] == base_id:
            return False

        raise error.AliasInUse(alias, ctx)

    tpc = TwoPhaseCommit(pool, insert_shard, 'set_alias',
            (base_id, ctx, digest_b64))
    try:
        with tpc as conn:
            timer.conn = conn
            inserted, owner_id = query.maybe_insert_alias_lookup(
                    conn.cursor(), digest, ctx, base_id, flags)

            if not inserted:
                tpc.fail()

                if owner_id == base_id:
                    return False

                raise error.AliasInUse(alias, ctx)

    except psycopg2.IntegrityError:
        owner = query.select_alias_lookup(conn.cursor(), digest, ctx)

        conn.rollback()

        if owner['base_id'] == base_id:
            return False

        raise error.AliasInUse(alias, ctx)

    finally:
        pool.put(conn)
        timer.conn = None

    with tpc.elsewhere():
        with pool.get_by_guid(base_id) as conn:
            timer.conn = conn
            try:
                result = query.insert_alias(
                        conn.cursor(), base_id, ctx, alias, index, flags)
            finally:
                timer.conn = None

            if not result:
                conn.rollback()
                tpc.fail()
                base_ctx = util.ctx_base_ctx(ctx)
                base_tbl = table.NAMES[util.ctx_tbl(base_ctx)]
                raise error.NoObject("%s<%d/%d>" %
                        (base_tbl, base_ctx, base_id))

    return True


def add_alias_flags(pool, base_id, ctx, alias, flags, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _add_alias_flags(pool, base_id, ctx, alias, flags, timer)
    with timer:
        return _add_alias_flags(pool, base_id, ctx, alias, flags, timer)

def _add_alias_flags(pool, base_id, ctx, alias, flags, timer):
    digest = hashlib.sha1(alias).digest()
    digest_b64 = digest.encode('base64').strip()

    for shard in pool.shards_for_lookup_hash(digest):
        with pool.get_by_shard(shard) as conn:
            timer.conn = conn
            try:
                owner = query.select_alias_lookup(conn.cursor(), digest, ctx)
            finally:
                timer.conn = None

            if owner is None:
                continue

            if owner['base_id'] != base_id:
                return None

            lookup_shard = shard
            break
    else:
        return None

    tpc = TwoPhaseCommit(pool, lookup_shard, 'add_alias_flags',
            (base_id, ctx, digest_b64, flags))
    try:
        with tpc as conn:
            timer.conn = conn
            cursor = conn.cursor()
            result = query.add_flags(cursor, 'alias_lookup', flags,
                    {'hash': digest, 'ctx': ctx})
            if not result:
                tpc.fail()
                return None
    finally:
        pool.put(conn)
        timer.conn = None

    result_flags = result[0]

    with tpc.elsewhere():
        with pool.get_by_guid(base_id) as conn:
            timer.conn = conn
            try:
                result = query.add_flags(conn.cursor(), 'alias', flags,
                        {'base_id': base_id, 'ctx': ctx, 'value': alias})
            finally:
                timer.conn = None

            if not result or result[0] != result_flags:
                conn.rollback()
                tpc.fail()
                return None

    return result_flags


def clear_alias_flags(pool, base_id, ctx, alias, flags, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _clear_alias_flags(pool, base_id, ctx, alias, flags, timer)
    with timer:
        return _clear_alias_flags(pool, base_id, ctx, alias, flags, timer)

def _clear_alias_flags(pool, base_id, ctx, alias, flags, timer):
    digest = hashlib.sha1(alias).digest()
    digest_b64 = digest.encode('base64').strip()

    for shard in pool.shards_for_lookup_hash(digest):
        with pool.get_by_shard(shard) as conn:
            timer.conn = conn
            try:
                owner = query.select_alias_lookup(conn.cursor(), digest, ctx)
            finally:
                timer.conn = None

            if owner is None:
                continue

            if owner['base_id'] != base_id:
                return None

            lookup_shard = shard
            break
    else:
        return None

    tpc = TwoPhaseCommit(pool, lookup_shard, 'clear_alias_flags',
            (base_id, ctx, digest_b64, flags))
    try:
        with tpc as conn:
            cursor = conn.cursor()
            timer.conn = conn
            try:
                result = query.clear_flags(cursor, 'alias_lookup', flags,
                        {'hash': digest, 'ctx': ctx})
            finally:
                timer.conn = None

            if not result:
                tpc.fail()
                return None
    finally:
        pool.put(conn)

    result_flags = result[0]

    with tpc.elsewhere():
        with pool.get_by_guid(base_id) as conn:
            timer.conn = conn
            try:
                result = query.clear_flags(conn.cursor(), 'alias', flags,
                        {'base_id': base_id, 'ctx': ctx})
            finally:
                timer.conn = None

            if not result or result[0] != result_flags:
                conn.rollback()
                tpc.fail()
                return None

    return result_flags


def remove_alias(pool, base_id, ctx, alias, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _remove_alias(pool, base_id, ctx, alias, timer)
    with timer:
        return _remove_alias(pool, base_id, ctx, alias, timer)

def _remove_alias(pool, base_id, ctx, alias, timer):
    digest = hashlib.sha1(alias).digest()
    digest_b64 = digest.encode('base64').strip()

    for shard in pool.shards_for_lookup_hash(digest):
        with pool.get_by_shard(shard) as conn:
            timer.conn = conn
            try:
                owner = query.select_alias_lookup(conn.cursor(), digest, ctx)
            finally:
                timer.conn = None

            if owner is None:
                continue

            if owner['base_id'] != base_id:
                return False

            lookup_shard = shard
            break
    else:
        return False

    tpc = TwoPhaseCommit(
            pool, lookup_shard, 'remove_alias', (base_id, ctx, digest_b64))
    try:
        with tpc as conn:
            cursor = conn.cursor()
            timer.conn = conn
            try:
                result = query.remove_alias_lookup(
                        cursor, digest, ctx, base_id)
            finally:
                timer.conn = None

            if not result:
                tpc.fail()
                return False
    finally:
        pool.put(conn)

    with tpc.elsewhere():
        with pool.get_by_guid(base_id) as conn:
            timer.conn = conn
            try:
                result = query.remove_alias(conn.cursor(), base_id, ctx, alias)
            finally:
                timer.conn = None

            if not result:
                conn.rollback()
                tpc.fail()
                return False

    return True


def create_relationship_pair(pool, base_id, rel_id, ctx, forw_idx, rev_idx,
        flags, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _create_relationship_pair(
                pool, base_id, rel_id, ctx, forw_idx, rev_idx, flags, timer)
    with timer:
        return _create_relationship_pair(
                pool, base_id, rel_id, ctx, forw_idx, rev_idx, flags, timer)

def _create_relationship_pair(pool, base_id, rel_id, ctx, forw_idx, rev_idx,
        flags, timer):
    tpc = TwoPhaseCommit(pool, pool.shard_by_guid(base_id),
            'create_relationship_pair', (base_id, rel_id, ctx))
    try:
        with tpc as conn:
            timer.conn = conn
            try:
                inserted = query.insert_relationship(conn.cursor(), base_id,
                        rel_id, ctx, True, forw_idx, flags)
            finally:
                timer.conn = None

            if not inserted:
                tpc.fail()

                base_ctx = util.ctx_base_ctx(ctx)
                base_tbl = table.NAMES[util.ctx_tbl(base_ctx)]
                raise error.NoObject("%s<%d/%d>" %
                        (base_tbl, base_ctx, base_id))

    except psycopg2.IntegrityError:
        return False

    finally:
        pool.put(conn)

    try:
        with tpc.elsewhere():
            with pool.get_by_guid(rel_id) as conn:
                timer.conn = conn
                try:
                    inserted = query.insert_relationship(conn.cursor(),
                            base_id, rel_id, ctx, False, rev_idx, flags)
                finally:
                    timer.conn = None

                if not inserted:
                    tpc.fail()

                    rel_ctx = util.ctx_rel_ctx(ctx)
                    rel_tbl = table.NAMES[util.ctx_tbl(rel_ctx)]
                    raise error.NoObject("%s<%d/%d>" %
                            (rel_tbl, rel_ctx, rel_id))

    except psycopg2.IntegrityError:
        return False

    return True


def add_relationship_flags(pool, base_id, rel_id, ctx, flags, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _add_relationship_flags(
                pool, base_id, rel_id, ctx, flags, timer)
    with timer:
        return _add_relationship_flags(
                pool, base_id, rel_id, ctx, flags, timer)

def _add_relationship_flags(pool, base_id, rel_id, ctx, flags, timer):
    tpc = TwoPhaseCommit(pool, pool.shard_by_guid(base_id),
            'add_relationship_flags', (base_id, rel_id, ctx, flags))
    try:
        with tpc as conn:
            timer.conn = conn
            try:
                result = query.add_flags(conn.cursor(), 'relationship', flags,
                        {'base_id': base_id, 'rel_id': rel_id, 'ctx': ctx,
                            'forward': True})
            finally:
                timer.conn = None

            if not result:
                tpc.fail()
                return None

    finally:
        pool.put(conn)

    result_flags = result[0]

    with tpc.elsewhere():
        with pool.get_by_guid(rel_id) as conn:
            timer.conn = conn
            try:
                result = query.add_flags(conn.cursor(), 'relationship', flags,
                        {'base_id': base_id, 'rel_id': rel_id, 'ctx': ctx,
                            'forward': False})
            finally:
                timer.conn = None

            if not result or result[0] != result_flags:
                conn.rollback()
                tpc.fail()
                return None

    return result_flags


def clear_relationship_flags(pool, base_id, rel_id, ctx, flags, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _clear_relationship_flags(
                pool, base_id, rel_id, ctx, flags, timer)
    with timer:
        return _clear_relationship_flags(
                pool, base_id, rel_id, ctx, flags, timer)

def _clear_relationship_flags(pool, base_id, rel_id, ctx, flags, timer):
    tpc = TwoPhaseCommit(pool, pool.shard_by_guid(base_id),
            'clear_relationship_flags', (base_id, rel_id, ctx, flags))
    try:
        with tpc as conn:
            timer.conn = conn
            try:
                result = query.clear_flags(conn.cursor(), 'relationship', flags,
                        {'base_id': base_id, 'rel_id': rel_id, 'ctx': ctx,
                            'forward': True})
            finally:
                timer.conn = None

            if not result:
                tpc.fail()
                return None

    finally:
        pool.put(conn)

    result_flags = result[0]

    with tpc.elsewhere():
        with pool.get_by_guid(rel_id) as conn:
            timer.conn = conn
            try:
                result = query.clear_flags(conn.cursor(), 'relationship', flags,
                        {'base_id': base_id, 'rel_id': rel_id, 'ctx': ctx,
                            'forward': False})
            finally:
                timer.conn = None

            if not result or result[0] != result_flags:
                conn.rollback()
                tpc.fail()
                return None

    return result_flags


def remove_relationship_pair(pool, base_id, rel_id, ctx, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _remove_relationship_pair(pool, base_id, rel_id, ctx, timer)
    with timer:
        return _remove_relationship_pair(pool, base_id, rel_id, ctx, timer)

def _remove_relationship_pair(pool, base_id, rel_id, ctx, timer):
    tpc = TwoPhaseCommit(pool, pool.shard_by_guid(base_id),
            'remove_relationship_pair', (base_id, rel_id, ctx))
    try:
        with tpc as conn:
            timer.conn = conn
            try:
                removed = query.remove_relationship(
                        conn.cursor(), base_id, rel_id, ctx, True)
            finally:
                timer.conn = None

            if not removed:
                tpc.fail()
                return False
    finally:
        pool.put(conn)

    with tpc.elsewhere():
        with pool.get_by_guid(rel_id) as conn:
            timer.conn = conn
            try:
                removed = query.remove_relationship(
                        conn.cursor(), base_id, rel_id, ctx, False)
            finally:
                timer.conn = None

            if not removed:
                conn.rollback()
                tpc.fail()
                return False
            return True


def create_node(pool, base_id, ctx, value, index, flags, timeout):
    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        cursor = conn.cursor()
        node = query.insert_node(cursor, base_id, ctx, value, flags)
        if node is None:
            return None

        query.insert_edge(cursor, base_id, ctx, node['guid'], index)
        return node


def move_node(pool, node_id, ctx, base_id, new_base_id, index, timeout):
    if pool.shard_by_guid(base_id) == pool.shard_by_guid(new_base_id):
        with pool.get_by_guid(base_id, timeout=timeout) as conn:
            cursor = conn.cursor()
            if not query.remove_edge(cursor, base_id, ctx, node_id):
                return False

            base_ctx = util.ctx_base_ctx(ctx)
            if not query.insert_edge(
                    cursor, new_base_id, ctx, node_id, index, base_ctx):
                conn.rollback()
                return False

        return True

    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _move_node(pool, node_id, ctx, base_id, new_base_id, timer)
    with timer:
        return _move_node(pool, node_id, ctx, base_id, new_base_id, timer)


def _move_node(pool, node_id, ctx, base_id, new_base_id, timer):
    base_ctx = util.ctx_base_ctx(ctx)

    tpc = TwoPhaseCommit(pool, pool.shard_by_guid(base_id), 'move_node',
            (node_id, ctx, base_id, new_base_id))
    try:
        with tpc as conn:
            timer.conn = conn
            if not query.remove_edge(
                    conn.cursor(), base_id, ctx, node_id):
                tpc.fail()
                return False
    finally:
        pool.put(conn)
        timer.conn = None

    with tpc.elsewhere():
        with pool.get_by_guid(new_base_id) as conn:
            timer.conn = conn
            try:
                if not query.insert_edge(conn.cursor(),
                        new_base_id, ctx, node_id, None, base_ctx):
                    tpc.fail()
                    return False
            finally:
                timer.conn = None

    return True


def create_name(pool, base_id, ctx, value, flags, index, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _create_name(pool, base_id, ctx, value, flags, index, timer)
    with timer:
        return _create_name(pool, base_id, ctx, value, flags, index, timer)

def _create_name(pool, base_id, ctx, value, flags, index, timer):
    base_ctx = util.ctx_base_ctx(ctx)

    tpc = TwoPhaseCommit(pool, pool.shard_by_guid(base_id), 'create_name',
            (base_id, ctx, value, flags, index))
    try:
        with tpc as conn:
            timer.conn = conn
            inserted = query.insert_name(
                    conn.cursor(), base_id, ctx, value, flags, index)

            if not inserted:
                tpc.fail()
                return False

    except psycopg2.IntegrityError:
        conn.rollback()
        return False

    finally:
        pool.put(conn)
        timer.conn = None

    with tpc.elsewhere():
        inserted = _write_name_lookup(
                pool, tpc, base_id, ctx, value, flags, timer)

    return True


def _write_name_lookup(pool, tpc, base_id, ctx, value, flags, timer):
    sclass = util.ctx_search(ctx)

    if sclass == search.PREFIX:
        return _write_prefix_lookup(pool, base_id, ctx, value, flags, timer)

    if sclass is None:
        raise error.BadContext(ctx)


def _write_prefix_lookup(pool, base_id, ctx, value, flags, timer):
    try:
        with pool.get_by_shard(pool.shard_for_prefix_write(value)) as conn:
            timer.conn = conn
            return query.insert_prefix_lookup(
                    conn.cursor(), value, flags, ctx, base_id)
    finally:
        timer.conn = None


def search_names(pool, value, ctx, limit, start, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _search_names(pool, value, ctx, limit, start, timer)
    with timer:
        return _search_names(pool, value, ctx, limit, start, timer)


def _search_names(pool, value, ctx, limit, start, timer):
    sclass = util.ctx_search(ctx)

    if sclass == search.PREFIX:
        return _search_prefix(pool, value, ctx, limit, start, timer)


def _search_prefix(pool, value, ctx, limit, start, timer):
    if start is None:
        start = ''

    names = []
    for shard in pool.shards_for_lookup_prefix(value):
        with pool.get_by_shard(shard) as conn:
            try:
                timer.conn = conn
                names.extend(query.search_prefixes(
                    conn.cursor(), value, ctx, limit, start))
            finally:
                timer.conn = None

    names.sort(key=lambda name: name['value'])
    return names[:limit]


def add_name_flags(pool, base_id, ctx, value, flags, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _add_name_flags(pool, base_id, ctx, value, flags, timer)
    with timer:
        return _add_name_flags(pool, base_id, ctx, value, flags, timer)


def _add_name_flags(pool, base_id, ctx, value, flags, timer):
    lookup_shard = _find_name_lookup_shard(pool, base_id, ctx, value, timer)
    if lookup_shard is None:
        return None

    tpc = TwoPhaseCommit(pool, pool.shard_by_guid(base_id), 'add_name_flags',
            (base_id, ctx, value, flags))

    try:
        with tpc as conn:
            timer.conn = conn
            result = query.add_flags(conn.cursor(), 'name', flags,
                    {'base_id': base_id, 'ctx': ctx, 'value': value})
            if not result:
                tpc.fail()
                return None

    finally:
        pool.put(conn)
        timer.conn = None

    result_flags = result[0]

    with tpc.elsewhere():
        if not _apply_flags_to_lookup(pool, lookup_shard, query.add_flags,
                flags, base_id, ctx, value, timer, result_flags):
            return None

    return result_flags


def clear_name_flags(pool, base_id, ctx, value, flags, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _clear_name_flags(pool, base_id, ctx, value, flags, timer)
    with timer:
        return _clear_name_flags(pool, base_id, ctx, value, flags, timer)


def _clear_name_flags(pool, base_id, ctx, value, flags, timer):
    lookup_shard = _find_name_lookup_shard(pool, base_id, ctx, value, timer)
    if lookup_shard is None:
        return None

    tpc = TwoPhaseCommit(pool, pool.shard_by_guid(base_id), 'clear_name_flags',
            (base_id, ctx, value, flags))

    try:
        with tpc as conn:
            timer.conn = conn
            result = query.clear_flags(conn.cursor(), 'name', flags,
                    {'base_id': base_id, 'ctx': ctx, 'value': value})
            if not result:
                tpc.fail()
                return None

    finally:
        pool.put(conn)
        timer.conn = None

    result_flags = result[0]

    with tpc.elsewhere():
        if not _apply_flags_to_lookup(pool, lookup_shard, query.clear_flags,
                flags, base_id, ctx, value, timer, result_flags):
            return None

    return result_flags


def _find_name_lookup_shard(pool, base_id, ctx, value, timer):
    sclass = util.ctx_search(ctx)

    if sclass == search.PREFIX:
        return _find_prefix_lookup_shard(pool, base_id, ctx, value, timer)

    raise error.BadContext(ctx)


def _find_prefix_lookup_shard(pool, base_id, ctx, value, timer):
    for shard in pool.shards_for_lookup_prefix(value):
        with pool.get_by_shard(shard) as conn:
            try:
                timer.conn = conn
                if query.select_prefix_lookups(
                        conn.cursor(), value, ctx, base_id):
                    return shard

            finally:
                timer.conn = None

    return None


def _apply_flags_to_lookup(
        pool, lookup_shard, func, flags, base_id, ctx, value, timer, expected):
    sclass = util.ctx_search(ctx)

    if sclass == search.PREFIX:
        return _apply_flags_to_prefix_lookup(pool, lookup_shard, func, flags,
                base_id, ctx, value, timer, expected)

    raise error.BadContext(ctx)


def _apply_flags_to_prefix_lookup(
        pool, lookup_shard, func, flags, base_id, ctx, value, timer, expected):
    with pool.get_by_shard(lookup_shard) as conn:
        timer.conn = conn
        try:
            result = func(conn.cursor(), 'prefix_lookup', flags,
                    {'base_id': base_id, 'ctx': ctx, 'value': value})
        finally:
            timer.conn = None

        if not result or result[0] != expected:
            conn.rollback()
            tpc.fail()
            return False

    return True


def remove_name(pool, base_id, ctx, value, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _remove_name(pool, base_id, ctx, value, timer)
    with timer:
        return _remove_name(pool, base_id, ctx, value, timer)


def _remove_name(pool, base_id, ctx, value, timer):
    lookup_shard = _find_name_lookup_shard(pool, base_id, ctx, value, timer)

    tpc = TwoPhaseCommit(pool, pool.shard_by_guid(base_id), 'remove_name',
            (base_id, ctx, value))

    try:
        with tpc as conn:
            timer.conn = conn
            if not query.remove_name(conn.cursor(), base_id, ctx, value):
                tpc.fail()
                return False
    finally:
        pool.put(conn)
        timer.conn = None

    with tpc.elsewhere():
        if not _remove_lookup(pool, lookup_shard, base_id, ctx, value, timer):
            tpc.fail()
            return False

    return True


def _remove_lookup(pool, lookup_shard, base_id, ctx, value, timer):
    sclass = util.ctx_search(ctx)

    if sclass == search.PREFIX:
        return _remove_prefix_lookup(
                pool, lookup_shard, base_id, ctx, value, timer)

    raise error.BadContext(ctx)


def _remove_prefix_lookup(pool, lookup_shard, base_id, ctx, value, timer):
    with pool.get_by_shard(lookup_shard) as conn:
        timer.conn = conn
        try:
            return query.remove_prefix_lookup(
                    conn.cursor(), base_id, ctx, value)
        finally:
            timer.conn = None


def _remove_lookups(cursor, triples):
    prefixes = []
    for triple in triples:
        if util.ctx_search(triple[1]) == search.PREFIX:
            prefixes.append(triple)

    removed = query.remove_prefix_lookups_multi(cursor, prefixes)

    return removed


def _remove_local_estates(shard, pool, cursor, estate, entity_base):
    guids = estate[shard][3][:]
    del estate[shard][3][:]

    while guids:
        if not entity_base:
            guids = query.remove_nodes(cursor, guids)
        entity_base = False

        query.remove_properties_multiple_bases(cursor, guids)

        aliases = query.remove_aliases_multiple_bases(cursor, guids)
        for value, ctx in aliases:
            # add each alias_lookup to every shard it *might* live on
            digest = hashlib.sha1(value).digest()
            for s in pool.shards_for_lookup_hash(digest):
                group = estate.setdefault(s, (set(), set(), [], []))[0]
                group.add((value, ctx))

        names = query.remove_names_multiple_bases(cursor, guids)
        for base_id, ctx, value in names:
            for s in pool.shards_for_lookup_prefix(value):
                group = estate.setdefault(s, (set(), set(), [], []))[1]
                group.add((base_id, ctx, value))

        removed_rels = query.remove_relationships_multiple_bases(cursor, guids)
        for base_id, ctx, forward, rel_id in removed_rels:
            # append each relationship to the shard at the rel_id end
            if forward:
                s = pool.shard_by_guid(rel_id)
            else:
                s = pool.shard_by_guid(base_id)
            item = (base_id, ctx, not forward, rel_id)
            estate.setdefault(s, (set(), set(), [], []))[2].append(item)

        children = query.remove_edges_multiple_bases(cursor, guids)
        for guid in children:
            # append each child node to its shard
            s = pool.shard_by_guid(guid)
            estate.setdefault(s, (set(), set(), [], []))[3].append(guid)

        guids = estate[shard][3][:]
        del estate[shard][3][:]

    alias_lookups, name_lookups, rels, guids = estate[shard]

    if alias_lookups:
        removed = query.remove_alias_lookups_multi(cursor, list(alias_lookups))
        for pair in removed:
            for s in pool.shards_for_lookup_hash(pair[0]):
                if s == shard:
                    continue
                estate[s][0].discard(pair)

    if name_lookups:
        removed = _remove_lookups(cursor, name_lookups)
        for triple in removed:
            for s in pool.shards_for_lookup_prefix(triple[2]):
                if s == shard:
                    continue
                estate[s][1].discard(triple)

    if rels:
        query.remove_relationships_multi(cursor, rels)

        forw, rev = set(), set()
        for base_id, ctx, forward, rel_id in rels:
            if forward:
                forw.add((base_id, ctx))
            else:
                rev.add((rel_id, ctx))
        if forw:
            query.bulk_reorder_relationships(cursor, forw, True)
        if rev:
            query.bulk_reorder_relationships(cursor, rev, False)


    estate.pop(shard)


def remove_entity(pool, guid, ctx, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _remove_entity(pool, guid, ctx, timer)
    with timer:
        return _remove_entity(pool, guid, ctx, timer)

def _remove_entity(pool, guid, ctx, timer):
    shard = pool.shard_by_guid(guid)
    tpc = TwoPhaseCommit(pool, shard, "remove_entity_start",
            (guid, ctx, shard))
    tpcs = [tpc]

    try:
        with tpc as conn:
            timer.conn = conn
            if not query.remove_entity(conn.cursor(), guid, ctx):
                tpc.fail()
                return False
    finally:
        pool.put(conn)
        timer.conn = None

    estates = {pool.shard_by_guid(guid): (set(), set(), [], [guid])}

    try:
        while estates:
            shard = next(iter(estates))
            tpc = TwoPhaseCommit(pool, shard, 'remove_entity_shard',
                    (guid, ctx, shard))
            tpcs.append(tpc)

            try:
                with tpc as conn:
                    _remove_local_estates(next(iter(estates)), pool,
                            conn.cursor(), estates, True)
            finally:
                pool.put(conn)
    except Exception:
        klass, exc, tb = sys.exc_info()
        for tpc in tpcs:
            try:
                tpc.rollback()
            except Exception:
                pass
        raise klass, exc, tb
    else:
        for tpc in tpcs:
            tpc.commit()

    return True


def remove_node(pool, guid, ctx, base_id, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _remove_node(pool, guid, ctx, base_id, timer)
    with timer:
        return _remove_node(pool, guid, ctx, base_id, timer)

def _remove_node(pool, guid, ctx, base_id, timer):
    shard = pool.shard_by_guid(base_id)
    tpc = TwoPhaseCommit(pool, shard, "remove_node_edge",
            (guid, ctx, base_id, shard))
    tpcs = [tpc]

    try:
        with tpc as conn:
            timer.conn = conn
            if not query.remove_edge(
                    conn.cursor(), base_id, ctx, guid):
                tpc.fail()
                return False
    finally:
        pool.put(conn)
        timer.conn = None

    estates = {pool.shard_by_guid(guid): (set(), set(), [], [guid])}

    try:
        while estates:
            shard = next(iter(estates))
            tpc = TwoPhaseCommit(pool, shard, 'remove_node_shard',
                    (guid, ctx, base_id, shard))
            tpcs.append(tpc)

            try:
                with tpc as conn:
                    _remove_local_estates(next(iter(estates)),
                            pool, conn.cursor(), estates, False)
            finally:
                pool.put(conn)
    except Exception:
        klass, exc, tb = sys.exc_info()
        for tpc in tpcs:
            try:
                tpc.rollback()
            except Exception:
                pass
        raise klass, exc, tb
    else:
        for tpc in tpcs:
            tpc.commit()

    return True

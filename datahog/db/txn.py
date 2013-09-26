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
from ..const import table, util


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
    for shard in pool.shards_for_alias_hash(digest):
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
    for shard in pool.shards_for_alias_hash(digest):
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
                base_tbl = tables.NAMES[util.ctx_tbl(base_ctx)]
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

    for shard in pool.shards_for_alias_hash(digest):
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

    for shard in pool.shards_for_alias_hash(digest):
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

    for shard in pool.shards_for_alias_hash(digest):
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
                base_tbl = tables.NAMES[util.ctx_tbl(base_ctx)]
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
                    rel_tbl = tables.NAMES[util.ctx_tbl(rel_ctx)]
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


def create_node(pool, base_id, ctx, value, flags, timeout):
    with pool.get_by_guid(base_id, timeout=timeout) as conn:
        cursor = conn.cursor()
        node = query.insert_node(cursor, base_id, ctx, value, flags)
        if node is None:
            return None

        query.insert_edge(cursor, base_id, ctx, node['guid'])
        return node


def move_node(pool, node_id, ctx, base_id, new_base_id, timeout):
    if pool.shard_by_guid(base_id) == pool.shard_by_guid(new_base_id):
        with pool.get_by_guid(base_id, timeout=timeout) as conn:
            cursor = conn.cursor()
            if not query.remove_edge(cursor, base_id, ctx, node_id):
                return False

            base_ctx = util.ctx_base_ctx(ctx)
            if not query.insert_edge(
                    cursor, new_base_id, ctx, node_id, base_ctx):
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
                if not query.insert_edge(
                        conn.cursor(), new_base_id, ctx, node_id, base_ctx):
                    tpc.fail()
                    return False
            finally:
                timer.conn = None

    return True


def _remove_local_estates(shard, pool, cursor, estate, first_round=True):
    alias_lookups, rels, guids = estate[shard]
    guids = guids[:]
    del estate[shard][2][:]

    while guids:
        if first_round:
            guids = query.remove_nodes(cursor, guids)
            first_round = False

        aliases = query.remove_aliases_multiple_bases(cursor, guids)
        for value, ctx in aliases:
            # add each alias_lookup to every shard it *might* live on
            digest = hashlib.sha1(value).digest()
            for s in pool.shards_for_alias_hash(digest):
                estate.setdefault(s, (set(), [], []))[0].add((value, ctx))

        removed_rels = query.remove_relationships_multiple_bases(cursor, guids)
        for base_id, ctx, forward, rel_id in removed_rels:
            # append each relationship to the shard at the rel_id end
            if forward:
                s = pool.shard_by_guid(rel_id)
            else:
                s = pool.shard_by_guid(base_id)
            item = (base_id, ctx, not forward, rel_id)
            estate.setdefault(s, (set(), [], []))[1].append(item)

        children = query.remove_edges_multiple_bases(cursor, guids)
        for pair in children:
            # append each child node to its shard
            s = pool.shard_by_guid(pair[0])
            estate.setdefault(s, (set(), [], []))[2].append(pair)

        guids = estate[shard][2][:]
        del estate[shard][2][:]

    if alias_lookups:
        removed = query.remove_alias_lookups_multi(cursor, list(alias_lookups))
        for pair in removed:
            for s in pool.shards_for_alias_hash(pair[0]):
                if s == shard:
                    continue
                estate[s][0].discard(pair)

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

    estates = {pool.shard_by_guid(guid): (set(), [], [guid])}

    try:
        while estates:
            shard = next(iter(estates))
            tpc = TwoPhaseCommit(pool, shard, 'remove_entity_shard',
                    (guid, ctx, shard))
            tpcs.append(tpc)

            try:
                with tpc as conn:
                    _remove_local_estates(next(iter(estates)), pool,
                            conn.cursor(), estates, False)
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

    estates = {pool.shard_by_guid(guid): (set(), [], [guid])}

    try:
        while estates:
            shard = next(iter(estates))
            tpc = TwoPhaseCommit(pool, shard, 'remove_node_shard',
                    (guid, ctx, base_id, shard))
            tpcs.append(tpc)

            try:
                with tpc as conn:
                    _remove_local_estates(
                        next(iter(estates)), pool, conn.cursor(), estates)
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

# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

import contextlib
import hashlib
import random
import sys

import psycopg2

from . import error, query
from .const import table, util


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


def set_property(conn, base_id, ctx, value, flags):
    cursor = conn.cursor()
    try:
        return query.upsert_property(cursor, base_id, ctx, value, flags)

    except psycopg2.IntegrityError:
        conn.rollback()
        updated = query.update_property(cursor, base_id, ctx, value, flags)
        return False, bool(updated)

    finally:
        conn.commit()


def lookup_alias(pool, digest, ctx):
    for conn in pool.get_for_alias_hash(digest):
        try:
            alias = query.select_alias_lookup(conn.cursor(), digest, ctx)
            if alias is not None:
                return alias

        finally:
            conn.rollback()
            pool.put(conn)

    return None


def set_alias(pool, base_id, ctx, alias, flags):
    digest = hashlib.sha1(alias).digest()
    digest_b64 = digest.encode('base64').strip()

    # look up pre-existing aliases on any but the current insert shard
    insert_shard = pool.shard_for_alias_write(digest)
    owner = None
    for shard in pool.shards_for_alias_hash(digest):
        if shard == insert_shard:
            continue

        with pool.get_by_shard(shard) as conn:
            owner = query.select_alias_lookup(conn.cursor(), digest, ctx)
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

    with tpc.elsewhere():
        with pool.get_by_guid(base_id) as conn:
            if not query.insert_alias(
                    conn.cursor(), base_id, ctx, alias, flags):
                conn.rollback()
                tpc.fail()
                base_ctx = util.ctx_base_ctx(ctx)
                base_tbl = tables.NAMES[util.ctx_tbl(base_ctx)]
                raise error.NoObject("%s<%d/%d>" %
                        (base_tbl, base_ctx, base_id))

    return True


def add_alias_flags(pool, base_id, ctx, alias, flags):
    digest = hashlib.sha1(alias).digest()
    digest_b64 = digest.encode('base64').strip()

    for shard in pool.shards_for_alias_hash(digest):
        with pool.get_by_shard(shard) as conn:
            owner = query.select_alias_lookup(conn.cursor(), digest, ctx)

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
            cursor = conn.cursor()
            result = query.add_flags(cursor, 'alias_lookup', flags,
                    {'hash': digest, 'ctx': ctx})
            if not result:
                tpc.fail()
                return None
    finally:
        pool.put(conn)

    result_flags = result[0]

    with tpc.elsewhere():
        with pool.get_by_guid(base_id) as conn:
            result = query.add_flags(conn.cursor(), 'alias', flags,
                    {'base_id': base_id, 'ctx': ctx, 'value': alias})
            if not result or result[0] != result_flags:
                conn.rollback()
                tpc.fail()
                return None

    return result_flags


def clear_alias_flags(pool, base_id, ctx, alias, flags):
    digest = hashlib.sha1(alias).digest()
    digest_b64 = digest.encode('base64').strip()

    for shard in pool.shards_for_alias_hash(digest):
        with pool.get_by_shard(shard) as conn:
            owner = query.select_alias_lookup(conn.cursor(), digest, ctx)

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
            result = query.clear_flags(cursor, 'alias_lookup', flags,
                    {'hash': digest, 'ctx': ctx})
            if not result:
                tpc.fail()
                return None
    finally:
        pool.put(conn)

    result_flags = result[0]

    with tpc.elsewhere():
        with pool.get_by_guid(base_id) as conn:
            result = query.clear_flags(conn.cursor(), 'alias', flags,
                    {'base_id': base_id, 'ctx': ctx})
            if not result or result[0] != result_flags:
                conn.rollback()
                tpc.fail()
                return None

    return result_flags


def remove_alias(pool, base_id, ctx, alias):
    digest = hashlib.sha1(alias).digest()
    digest_b64 = digest.encode('base64').strip()

    for shard in pool.shards_for_alias_hash(digest):
        with pool.get_by_shard(shard) as conn:
            owner = query.select_alias_lookup(conn.cursor(), digest, ctx)

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
            if not query.remove_alias_lookup(cursor, digest, ctx, base_id):
                tpc.fail()
                return False
    finally:
        pool.put(conn)

    with tpc.elsewhere():
        with pool.get_by_guid(base_id) as conn:
            if not query.remove_alias(conn.cursor(), base_id, ctx, alias):
                conn.rollback()
                tpc.fail()
                return False

    return True


def create_relationship_pair(pool, base_id, rel_id, ctx, flags):
    tpc = TwoPhaseCommit(pool, conns.shard_by_guid(base_id),
            'create_relationship_pair', (base_id, rel_id, ctx))
    try:
        with tpc as conn:
            inserted = query.insert_relationship(
                    conn.cursor(), base_id, rel_id, ctx, True, flags)
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
                inserted = query.insert_relationship(
                        conn.cursor(), base_id, rel_id, ctx, False, flags)
                if not inserted:
                    tpc.fail()

                    rel_ctx = util.ctx_rel_ctx(ctx)
                    rel_tbl = tables.NAMES[util.ctx_tbl(rel_ctx)]
                    raise error.NoObject("%s<%d/%d>" %
                            (rel_tbl, rel_ctx, rel_id))

    except psycopg2.IntegrityError:
        return False

    return True


def add_relationship_flags(pool, base_id, rel_id, ctx, flags):
    tpc = TwoPhaseCommit(pool, conns.shard_by_guid(base_id),
            'add_relationship_flags', (base_id, rel_id, ctx, flags))
    try:
        with tpc as conn:
            result = query.add_flags(conn.cursor(), 'relationship', flags,
                    {'base_id': base_id, 'rel_id': rel_id, 'ctx': ctx,
                        'forward': True})
            if not result:
                tpc.fail()
                return None

    finally:
        pool.put(conn)

    result_flags = result[0]

    with tpc.elsewhere():
        with pool.get_by_guid(rel_id) as conn:
            result = query.add_flags(conn.cursor(), 'relationship', flags,
                    {'base_id': base_id, 'rel_id': rel_id, 'ctx': ctx,
                        'forward': False})
            if not result or result[0] != result_flags:
                conn.rollback()
                tpc.fail()
                return None

    return result_flags


def clear_relationship_flags(pool, base_id, rel_id, ctx, flags):
    tpc = TwoPhaseCommit(pool, conns.shard_by_guid(base_id),
            'clear_relationship_flags', (base_id, rel_id, ctx, flags))
    try:
        with tpc as conn:
            result = query.clear_flags(conn.cursor(), 'relationship', flags,
                    {'base_id': base_id, 'rel_id': rel_id, 'ctx': ctx,
                        'forward': True})
            if not result:
                tpc.fail()
                return None

    finally:
        pool.put(conn)

    result_flags = result[0]

    with tpc.elsewhere():
        with pool.get_by_guid(rel_id) as conn:
            result = query.clear_flags(conn.cursor(), 'relationship', flags,
                    {'base_id': base_id, 'rel_id': rel_id, 'ctx': ctx,
                        'forward': False})
            if not result or result[0] != result_flags:
                conn.rollback()
                tpc.fail()
                return None

    return result_flags


def remove_relationship_pair(pool, base_id, rel_id, ctx):
    tpc = TwoPhaseCommit(pool, conns.shard_by_guid(base_id),
            'remove_relationship_pair', (base_id, rel_id, ctx))
    try:
        with tpc as conn:
            if not query.remove_relationship(
                    conn.cursor(), base_id, rel_id, ctx, True):
                tpc.fail()
                return False
    finally:
        pool.put(conn)

    with tpc.elsewhere():
        with pool.get_by_guid(rel_id) as conn:
            if not query.remove_relationship(
                    conn.cursor(), base_id, rel_id, ctx, False):
                conn.rollback()
                tpc.fail()
                return False
            return True


def _remove_guids_phase2(cursor, guids, home_shard):
    props = query.remove_properties_multiple_bases(cursor, guids)

    aliases = query.remove_aliases_multiple_bases(cursor, guids)

    rels = query.remove_relationships_multiple_bases(cursor, guids)

    alias_shards = {}
    shards = {}
    for value, ctx in aliases:
        digest = hashlib.sha1(value).digest()
        shardlist = list(conns.shards_by_aliashash(digest))
        alias_shards[(digest, ctx)] = shardlist
        for shard in shardlist:
            group = shards.setdefault(shard, (set(), []))[0]
            group.add((digest, ctx))

    for base_id, ctx, forward, rel_id in rels:
        shard = conns.shard_by_guid(rel_id)
        if shard == home_shard:
            continue

        group = shards.setdefault(shard, (set(), []))[1]
        group.append((base_id, ctx, not forward, rel_id))

    return shards, alias_shards


def _remove_guids_phase3(
        cursor, aliases, rels, shard, shards, alias_shards, visited):
    if aliases:
        removed = query.remove_alias_lookups_multi(cursor, list(aliases))
        for r in removed:
            for othershard in alias_shards[r]:
                if othershard in visited:
                    continue
                shards[othershard][0].discard(r)

    if rels:
        query.remove_relationships_multi(cursor, rels)


def remove_entity(pool, guid, ctx):
    shard = pool.shard_by_guid(guid)
    tpc_base = TwoPhaseCommit(pool, shard, 'remove_entity_base', (guid,))
    conn = None
    try:
        with tpc_base as conn:
            cursor = conn.cursor()

            if not query.remove_entity(cursor, guid, ctx):
                tpc_base.fail()
                return False

            nids = query.remove_tree_node_descendents_of(cursor, guid)

            visited = set([shard])
            shards, alias_shards = _remove_guids_phase2(
                    cursor, [guid] + nids, shard)

    finally:
        if conn is not None:
            pool.put(conn)

    del conn, cursor

    with tpc_base.elsewhere():
        foreign_tpcs = []

        try:
            for shard, (aliases, rels) in shards.items():
                visited.add(shard)

                if not (aliases or rels):
                    continue

                tpc = TwoPhaseCommit(
                        pool, shard, 'remove_entity_foreign', (guid,))
                foreign_tpcs.append(tpc)

                try:
                    with tpc as conn:
                        _remove_guids_phase3(conn.cursor(), aliases, rels,
                                shard, shards, alias_shards, visited)

                finally:
                    pool.put(conn)

        except Exception:
            klass, exc, tb = sys.exc_info()
            try:
                for tpc in foreign_tpcs:
                    tpc.rollback()
            except Exception:
                pass

            raise klass, exc, tb

        else:
            for tpc in foreign_tpcs:
                tpc.commit()

    return True


def remove_tree_node(pool, guid, ctx, base_id):
    shard = pool.shard_by_guid(guid)
    tpc_base = TwoPhaseCommit(pool, shard, 'remove_tree_node_base', (guid,))
    conn = None
    try:
        with tpc_base as conn:
            cursor = conn.cursor()

            if not query.remove_tree_node(cursor, guid, ctx, base_id):
                tpc_base.fail()
                return False

            desc = query.remove_tree_node_descendents_of(cursor, guid)
            visited = set([shard])
            shards, alias_shards = _remove_guids_phase2(
                    cursor, [guid] + desc, shard)

    finally:
        if conn is not None:
            pool.put(conn)

    del conn, cursor

    with tpc_base.elsewhere():
        foreign_tpcs = []

        try:
            for shard, (aliases, rels) in shards.items():
                visited.add(shard)

                if not (aliases or rels):
                    continue

                tpc = TwoPhaseCommit(
                        pool, shard, 'remove_tree_node_foreign', (guid,))
                foreign_tpcs.append(tpc)

                conn = None
                try:
                    with tpc as conn:
                        _remove_guids_phase3(conn.cursor(), aliases, rels,
                                shard, shards, alias_shards, visited)

                finally:
                    if conn is not None:
                        pool.put(conn)

        except Exception:
            klass, exc, tb = sys.exc_info()
            try:
                for tpc in foreign_tpcs:
                    tpc.rollback()
            except Exception:
                pass

            raise klass, exc, tb

        else:
            for tpc in foreign_tpcs:
                tpc.commit()

    return True

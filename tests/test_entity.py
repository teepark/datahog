# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import hashlib
import hmac
import os
import sys
import unittest

import datahog

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import base
from pgmock import *


class EntityTests(base.TestCase):
    def setUp(self):
        super(EntityTests, self).setUp()
        datahog.set_context(1, datahog.ENTITY)

    def test_create(self):
        add_fetch_result([(45,)])
        self.assertEqual(datahog.entity.create(self.p, 1),
                {'guid': 45, 'flags': set(), 'ctx': 1})
        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
insert into entity (ctx, flags)
values (%s, %s)
returning guid""", (1, 0)),
            FETCH_ONE,
            COMMIT])

    def test_get_success(self):
        add_fetch_result([(0,)])
        self.assertEqual(datahog.entity.get(self.p, 12, 1),
                {'guid': 12, 'ctx': 1, 'flags': set()})
        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select flags from entity
where
    time_removed is null
    and guid=%s and ctx=%s
""", (12, 1)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT])

    def test_get_failure(self):
        add_fetch_result([])
        self.assertIsNone(datahog.entity.get(self.p, 12, 1))
        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select flags from entity
where
    time_removed is null
    and guid=%s and ctx=%s
""", (12, 1)),
            ROWCOUNT,
            COMMIT])

    def test_batch_get(self):
        add_fetch_result([(1, 1, 0), (2, 1, 0), (3, 1, 0)])
        self.assertEqual(
                datahog.entity.batch_get(self.p, [(1, 1), (2, 1), (3, 1)]),
                [{'guid': 1, 'ctx': 1, 'flags': set()},
                {'guid': 2, 'ctx': 1, 'flags': set()},
                {'guid': 3, 'ctx': 1, 'flags': set()}])
        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select guid, ctx, flags from entity
where
    time_removed is null
    and (guid, ctx) in ((%s, %s),(%s, %s),(%s, %s))
""", [1, 1, 2, 1, 3, 1]),
            FETCH_ALL,
            COMMIT])

    def test_batch_get_out_of_order(self):
        add_fetch_result([(3, 1, 0), (1, 1, 0), (2, 1, 0)])
        self.assertEqual(
                datahog.entity.batch_get(self.p, [(1, 1), (2, 1), (3, 1)]),
                [{'guid': 1, 'ctx': 1, 'flags': set()},
                {'guid': 2, 'ctx': 1, 'flags': set()},
                {'guid': 3, 'ctx': 1, 'flags': set()}])
        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select guid, ctx, flags from entity
where
    time_removed is null
    and (guid, ctx) in ((%s, %s),(%s, %s),(%s, %s))
""", [1, 1, 2, 1, 3, 1]),
            FETCH_ALL,
            COMMIT])

    def test_batch_get_missing(self):
        add_fetch_result([(3, 1, 0)])
        self.assertEqual(
                datahog.entity.batch_get(self.p, [(1, 1), (2, 1), (3, 1)]),
                [None, None, {'guid': 3, 'ctx': 1, 'flags': set()}])
        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select guid, ctx, flags from entity
where
    time_removed is null
    and (guid, ctx) in ((%s, %s),(%s, %s),(%s, %s))
""", [1, 1, 2, 1, 3, 1]),
            FETCH_ALL,
            COMMIT])

    def test_add_flags(self):
        datahog.set_flag(1, 1)
        datahog.set_flag(2, 1)
        datahog.set_flag(3, 1)
        add_fetch_result([(5,)])
        self.assertEqual(
                datahog.entity.add_flags(self.p, 17, 1, set([1, 3])),
                set([1, 3]))
        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update entity
set flags=flags | %s
where
    time_removed is null
    and guid=%s and ctx=%s
returning flags
""", [5, 17, 1]),
            FETCH_ALL,
            COMMIT])

    def test_add_flags_one_already_present(self):
        datahog.set_flag(1, 1)
        datahog.set_flag(2, 1)
        datahog.set_flag(3, 1)
        add_fetch_result([(7,)])
        self.assertEqual(
                datahog.entity.add_flags(self.p, 17, 1, set([1, 3])),
                set([1, 2, 3]))
        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update entity
set flags=flags | %s
where
    time_removed is null
    and guid=%s and ctx=%s
returning flags
""", [5, 17, 1]),
            FETCH_ALL,
            COMMIT])

    def test_clear_flags(self):
        datahog.set_flag(1, 1)
        datahog.set_flag(2, 1)
        datahog.set_flag(3, 1)
        add_fetch_result([(1,)])
        self.assertEqual(
                datahog.entity.clear_flags(self.p, 17, 1, set([2, 3])),
                set([1]))
        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update entity
set flags=flags & ~%s
where
    time_removed is null
    and guid=%s and ctx=%s
returning flags
""", [6, 17, 1]),
            FETCH_ALL,
            COMMIT])

    def test_remove_lone_ent(self):
        add_fetch_result([None]) # query.remove_entity rowcount
        add_fetch_result([]) # query.remove_properties_multiple_bases rowcount
        add_fetch_result([]) # query.remove_alias_multiple_bases fetchall
        add_fetch_result([]) # query.remove_names_multiple_bases fetchall
        add_fetch_result([]) # query.remove_relationships_multiple_bases fetchall
        add_fetch_result([]) # query.remove_edges_multiple_bases fetchall
        guid = 121
        ctx = 1
        self.assertEqual(
                datahog.entity.remove(self.p, guid, ctx),
                True)
        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update entity
set time_removed=now()
where
    time_removed is null
    and guid=%s
    and ctx=%s
""", (guid, ctx)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update property
set time_removed=now()
where
    time_removed is null
    and base_id in (%s)
""", (guid,)),
            ROWCOUNT,
            EXECUTE("""
update alias
set time_removed=now()
where
    time_removed is null
    and base_id in (%s)
returning value, ctx
""", (guid,)),
            FETCH_ALL,
            EXECUTE("""
update name
set time_removed=now()
where
    time_removed is null
    and base_id in (%s)
returning base_id, ctx, value
""", (guid,)),
            FETCH_ALL,
            EXECUTE("""
with forwardrels (base_id, ctx, forward, rel_id) as (
    update relationship
    set time_removed=now()
    where
        time_removed is null
        and forward=true
        and base_id in (%s)
    returning base_id, ctx, forward, rel_id
),
backwardrels (base_id, ctx, forward, rel_id) as (
    update relationship
    set time_removed=now()
    where
        time_removed is null
        and forward=false
        and rel_id in (%s)
    returning base_id, ctx, forward, rel_id
)
select base_id, ctx, forward, rel_id from forwardrels
UNION ALL
select base_id, ctx, forward, rel_id from backwardrels
""", (guid, guid)),
            FETCH_ALL,
            EXECUTE("""
update edge
set time_removed=now()
where
    time_removed is null
    and base_id in (%s)
returning child_id
""", (guid,)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            TPC_COMMIT,
            TPC_COMMIT])

    def test_remove_failure(self):
        add_fetch_result([]) # query.remove_entity rowcount
        guid = 4834
        ctx = 1
        self.assertEqual(
                datahog.entity.remove(self.p, guid, ctx),
                False)
        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update entity
set time_removed=now()
where
    time_removed is null
    and guid=%s
    and ctx=%s
""", (guid, ctx)),
            ROWCOUNT,
            TPC_ROLLBACK])

    def test_remove_with_estate(self):
        datahog.set_context(2, datahog.PROPERTY, {
            'base_ctx': 1, 'storage': datahog.storage.INT
        })
        datahog.set_context(3, datahog.ALIAS, {'base_ctx': 1})
        datahog.set_context(4, datahog.NODE, {
            'base_ctx': 1, 'storage': datahog.storage.INT
        })
        datahog.set_context(5, datahog.NODE, {
            'base_ctx': 4, 'storage': datahog.storage.INT
        })
        datahog.set_context(6, datahog.RELATIONSHIP, {
            'base_ctx': 5, 'rel_ctx': 5
        })
        datahog.set_context(7, datahog.ALIAS, {'base_ctx': 4})

        # query.remove_entity rowcount
        add_fetch_result([None])

        # query.remove_properties_multiple_bases rowcount
        add_fetch_result([None, None])

        # query.remove_alias_multiple_bases fetchall
        add_fetch_result([('value1', 3), ('value2', 3)])

        # query.remove_names_multiple_bases fetchall
        add_fetch_result([])

        # query.remove_relationships_multiple_bases fetchall
        add_fetch_result([])

        # query.remove_edges_multiple_bases fetchall
        add_fetch_result([(1278,), (43892,)])

        #
        # context 4 nodes
        #
        # query.remove_nodes fetchall
        add_fetch_result([(1278,), (43892,)])

        # query.remove_properties_multiple_bases rowcount
        add_fetch_result([])

        # query.remove_aliases_multiple_bases fetchall
        add_fetch_result([('nodeval', 7)])

        # query.remove_names_multiple_bases fetchall
        add_fetch_result([])

        # query.remove_relationships_multiple_bases fetchall
        add_fetch_result([])

        # query.remove_edges_multiple_bases fetchall
        add_fetch_result([(78938,), (29881,)])

        # 
        # context 5 nodes
        # 
        # query.remove_nodes fetchall
        add_fetch_result([(78938,), (29881,)])

        # query.remove_properties_multiple_bases rowcount
        add_fetch_result([])

        # query.remove_alias_multiple_bases fetchall
        add_fetch_result([])

        # query.remove_names_multiple_bases fetchall
        add_fetch_result([])

        # query.remove_relationships_multiple_bases fetchall
        add_fetch_result([
            (78938, 6, True, 29881),
            (29881, 6, True, 78938),
            (78938, 6, False, 29881),
            (29881, 6, False, 78938),
        ])

        # query.remove_edges_multiple_bases fetchall
        add_fetch_result([])

        # query.remove_alias_lookups_multi fetchall
        digests = [hmac.new(self.p.digestkey, x, hashlib.sha1).digest()
                for x in ['value1', 'value2', 'nodeval']]
        add_fetch_result(zip(digests, [3, 3, 7]))
        flatlookups = []
        map(flatlookups.extend, set(zip(digests, [3, 3, 7])))

        guid = 1234
        ctx = 1

        self.assertEqual(
                datahog.entity.remove(self.p, guid, ctx),
                True)

        estate_iteration = lambda guids: [
            EXECUTE("""
update node
set time_removed=now()
where
    time_removed is null
    and guid in (%s)
returning guid
""" % (','.join('%s' for g in guids),), guids),
            FETCH_ALL,
            EXECUTE("""
update property
set time_removed=now()
where
    time_removed is null
    and base_id in (%s)
""" % (','.join('%s' for g in guids),), guids),
            ROWCOUNT,
            EXECUTE("""
update alias
set time_removed=now()
where
    time_removed is null
    and base_id in (%s)
returning value, ctx
""" % (','.join('%s' for g in guids),), guids),
            FETCH_ALL,
            EXECUTE("""
update name
set time_removed=now()
where
    time_removed is null
    and base_id in (%s)
returning base_id, ctx, value
""" % (','.join('%s' for g in guids),), guids),
            FETCH_ALL,
            EXECUTE("""
with forwardrels (base_id, ctx, forward, rel_id) as (
    update relationship
    set time_removed=now()
    where
        time_removed is null
        and forward=true
        and base_id in (%s)
    returning base_id, ctx, forward, rel_id
),
backwardrels (base_id, ctx, forward, rel_id) as (
    update relationship
    set time_removed=now()
    where
        time_removed is null
        and forward=false
        and rel_id in (%s)
    returning base_id, ctx, forward, rel_id
)
select base_id, ctx, forward, rel_id from forwardrels
UNION ALL
select base_id, ctx, forward, rel_id from backwardrels
""" % ((','.join('%s' for g in guids),) * 2), guids * 2),
            FETCH_ALL,
            EXECUTE("""
update edge
set time_removed=now()
where
    time_removed is null
    and base_id in (%s)
returning child_id
""" % (','.join('%s' for g in guids),), guids),
            FETCH_ALL]

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update entity
set time_removed=now()
where
    time_removed is null
    and guid=%s
    and ctx=%s
""", (guid, ctx)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            TPC_BEGIN,
            GET_CURSOR] +
            estate_iteration([guid])[2:] +
            estate_iteration([1278, 43892]) +
            estate_iteration([78938, 29881]) +
        [
            EXECUTE("""
update alias_lookup
set time_removed=now()
where
    time_removed is null
    and (hash, ctx) in ((%s, %s),(%s, %s),(%s, %s))
returning hash, ctx
""", flatlookups),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            TPC_COMMIT,
            TPC_COMMIT])

if __name__ == '__main__':
    unittest.main()

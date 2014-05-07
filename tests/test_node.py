# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import os
import sys
import unittest

import datahog

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import base
from pgmock import *


class NodeTests(base.TestCase):
    def setUp(self):
        super(NodeTests, self).setUp()
        datahog.set_context(1, datahog.ENTITY)
        datahog.set_context(2, datahog.NODE, {
            'base_ctx': 1, 'storage': datahog.storage.INT
        })

    def test_create(self):
        add_fetch_result([(1234,)])
        add_fetch_result([(1,)])
        self.assertEqual(
            datahog.node.create(self.p, 123, 2, 12),
            {'guid': 1234, 'ctx': 2, 'value': 12, 'flags': set()})

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
insert into node (ctx, num, flags)
select %s, %s, %s
where exists (
    select 1
    from entity
    where
        time_removed is null
        and guid=%s
        and ctx=%s
)
returning guid
""", (2, 12, 0, 123, 1)),
            ROWCOUNT,
            FETCH_ONE,
            EXECUTE("""
insert into edge (base_id, ctx, child_id, pos)
select %s, %s, %s, (
    select count(*)
    from edge
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
)
where true
""", (123, 2, 1234, 123, 2)),
            ROWCOUNT,
            COMMIT])

    def test_create_at_index(self):
        add_fetch_result([(1234,)])
        add_fetch_result([(1,)])
        self.assertEqual(
                datahog.node.create(self.p, 123, 2, 12, 5),
                {'guid': 1234, 'ctx': 2, 'value': 12, 'flags': set()})

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
insert into node (ctx, num, flags)
select %s, %s, %s
where exists (
    select 1
    from entity
    where
        time_removed is null
        and guid=%s
        and ctx=%s
)
returning guid
""", (2, 12, 0, 123, 1)),
            ROWCOUNT,
            FETCH_ONE,
            EXECUTE("""
with bump as (
    update edge
    set pos=pos + 1
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and pos >= %s
        and true
)
insert into edge (base_id, ctx, child_id, pos)
select %s, %s, %s, %s
where true
returning 1
""", (123, 2, 5, 123, 2, 1234, 5)),
            ROWCOUNT,
            COMMIT])

    def test_get(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)
        add_fetch_result([(7, 4781)])

        self.assertEqual(
                datahog.node.get(self.p, 34789, 2),
                {'guid': 34789, 'ctx': 2, 'value': 4781, 'flags': set([1, 2, 3])})

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select flags, num
from node
where
    time_removed is null
    and guid=%s
    and ctx=%s
""", (34789, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT])

    def test_batch_get(self):
        datahog.set_context(3, datahog.NODE, {
            'base_ctx': 1, 'storage': datahog.storage.STR
        })
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)
        add_fetch_result([
            (1234, 2, 5, 3478, None),
            (1237, 3, 0, None, "string value"),
            (1236, 2, 1, 3782, None),
        ])

        self.assertEqual(
                datahog.node.batch_get(self.p, [
                    (1234, 2), (1235, 2), (1236, 2), (1237, 3)]),
                [{'guid': 1234, 'ctx': 2, 'flags': set([1, 3]), 'value': 3478},
                None,
                {'guid': 1236, 'ctx': 2, 'flags': set([1]), 'value': 3782},
                {'guid': 1237, 'ctx': 3, 'flags': set(), 'value': 'string value'}])

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select guid, ctx, flags, num, value
from node
where
    time_removed is null
    and (guid, ctx) in ((%s,%s), (%s,%s), (%s,%s), (%s,%s))
""", (1234, 2, 1235, 2, 1236, 2, 1237, 3)),
            FETCH_ALL,
            COMMIT])

    def test_child_of_success(self):
        add_fetch_result([(1,)])

        self.assertEqual(
                datahog.node.child_of(self.p, 1234, 2, 123),
                True)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select 1
from edge
where
    time_removed is null
    and child_id=%s
    and ctx=%s
    and base_id=%s
""", (1234, 2, 123)),
            ROWCOUNT,
            COMMIT])

    def test_child_of_failure(self):
        add_fetch_result([])

        self.assertEqual(
                datahog.node.child_of(self.p, 1234, 2, 125),
                False)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select 1
from edge
where
    time_removed is null
    and child_id=%s
    and ctx=%s
    and base_id=%s
""", (1234, 2, 125)),
            ROWCOUNT,
            COMMIT])

    def test_list_children(self):
        add_fetch_result([
            (1234, 2, 0),
            (1235, 2, 1),
            (1236, 2, 2)])

        self.assertEqual(
                datahog.node.list_children(self.p, 1233, 2),
                ([1234, 1235, 1236], 3))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select child_id, ctx, pos
from edge
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and pos >= %s
order by pos asc
limit %s
""", (1233, 2, 0, 100)),
            FETCH_ALL,
            COMMIT])

    def test_get_children(self):
        add_fetch_result([
            (1234, 2, 0),
            (1235, 2, 1),
            (1236, 2, 2)
        ])
        add_fetch_result([
            (1235, 2, 0, 742, None),
            (1234, 2, 0, 87422, None),
            (1236, 2, 0, 8928, None),
        ])

        self.assertEqual(
                datahog.node.get_children(self.p, 1233, 2),
                ([
                    {'guid': 1234, 'ctx': 2, 'value': 87422, 'flags': set()},
                    {'guid': 1235, 'ctx': 2, 'value': 742, 'flags': set()},
                    {'guid': 1236, 'ctx': 2, 'value': 8928, 'flags': set()}
                ], 3))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select child_id, ctx, pos
from edge
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and pos >= %s
order by pos asc
limit %s
""", (1233, 2, 0, 100)),
            FETCH_ALL,
            COMMIT,
            GET_CURSOR,
            EXECUTE("""
select guid, ctx, flags, num, value
from node
where
    time_removed is null
    and (guid, ctx) in ((%s, %s),(%s, %s),(%s, %s))
""", (1234, 2, 1235, 2, 1236, 2)),
            FETCH_ALL,
            COMMIT])

    def test_update_success(self):
        add_fetch_result([None]) # for rowcount

        self.assertEqual(
                datahog.node.update(self.p, 1234, 2, 12),
                True)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update node
set num=%s, value=null
where
    time_removed is null
    and guid=%s
    and ctx=%s
""", (12, 1234, 2)),
            ROWCOUNT,
            COMMIT])

    def test_update_failure(self):
        add_fetch_result([]) # rowcount 0

        self.assertEqual(
                datahog.node.update(self.p, 1234, 2, 12),
                False)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update node
set num=%s, value=null
where
    time_removed is null
    and guid=%s
    and ctx=%s
""", (12, 1234, 2)),
            ROWCOUNT,
            COMMIT])

    def test_update_with_oldval(self):
        add_fetch_result([]) # rowcount 0

        self.assertEqual(
                datahog.node.update(self.p, 1234, 2, 12, 77),
                False)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update node
set num=%s, value=null
where
    time_removed is null
    and guid=%s
    and ctx=%s
    and num=%s
""", (12, 1234, 2, 77)),
            ROWCOUNT,
            COMMIT])

    def test_increment(self):
        add_fetch_result([(15,)])

        self.assertEqual(
                datahog.node.increment(self.p, 1234, 2),
                15)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update node
set num=num+%s
where
    time_removed is null
    and guid=%s
    and ctx=%s
returning num
""", (1, 1234, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT])

    def test_increment_limit_pos(self):
        add_fetch_result([(20,)])

        self.assertEqual(
                datahog.node.increment(self.p, 1234, 2, by=3, limit=20),
                20)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update node
set num=case
    when (num+%s < %s)
    then num+%s
    else %s
    end
where
    time_removed is null
    and guid=%s
    and ctx=%s
returning num
""", (3, 20, 3, 20, 1234, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT])

    def test_increment_limit_neg(self):
        add_fetch_result([(20,)])

        self.assertEqual(
                datahog.node.increment(self.p, 1234, 2, by=-3, limit=20),
                20)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update node
set num=case
    when (num+%s > %s)
    then num+%s
    else %s
    end
where
    time_removed is null
    and guid=%s
    and ctx=%s
returning num
""", (-3, 20, -3, 20, 1234, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT])

    def test_add_flags(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2 ,2)
        datahog.set_flag(3, 2)
        add_fetch_result([(5,)])

        self.assertEqual(
                datahog.node.add_flags(self.p, 1324, 2, set([1, 3])),
                set([1, 3]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update node
set flags=flags | %s
where
    time_removed is null
    and guid=%s and ctx=%s
returning flags
""", (5, 1324, 2)),
            FETCH_ALL,
            COMMIT])

    def test_add_flags_one_already_present(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2 ,2)
        datahog.set_flag(3, 2)
        add_fetch_result([(7,)])

        self.assertEqual(
                datahog.node.add_flags(self.p, 1324, 2, set([1, 3])),
                set([1, 2, 3]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update node
set flags=flags | %s
where
    time_removed is null
    and guid=%s and ctx=%s
returning flags
""", (5, 1324, 2)),
            FETCH_ALL,
            COMMIT])

    def test_clear_flags(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)
        add_fetch_result([(1,)])

        self.assertEqual(
                datahog.node.clear_flags(self.p, 1234, 2, set([2, 3])),
                set([1]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update node
set flags=flags & ~%s
where
    time_removed is null
    and guid=%s and ctx=%s
returning flags
""", (6, 1234, 2)),
            FETCH_ALL,
            COMMIT])

    def test_shift(self):
        add_fetch_result([(True,)])

        self.assertEqual(
                datahog.node.shift(self.p, 1234, 2, 123, 0),
                True)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
with oldpos as (
    select pos
    from edge
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and child_id=%s
), bump as (
    update edge
    set pos=pos + (case
        when (select pos from oldpos) < pos
        then -1
        else 1
        end)
    where
        exists (select 1 from oldpos)
        and time_removed is null
        and base_id=%s
        and ctx=%s
        and pos between symmetric (select pos from oldpos) and %s
), move as (
    update edge
    set pos=%s
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and child_id=%s
    returning 1
)
select exists (select 1 from move)
""", (123, 2, 1234, 123, 2, 0, 0, 123, 2, 1234)),
            FETCH_ONE,
            COMMIT])

    def test_shift_failure(self):
        add_fetch_result([(False,)])

        self.assertEqual(
                datahog.node.shift(self.p, 1234, 2, 123, 0),
                False)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
with oldpos as (
    select pos
    from edge
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and child_id=%s
), bump as (
    update edge
    set pos=pos + (case
        when (select pos from oldpos) < pos
        then -1
        else 1
        end)
    where
        exists (select 1 from oldpos)
        and time_removed is null
        and base_id=%s
        and ctx=%s
        and pos between symmetric (select pos from oldpos) and %s
), move as (
    update edge
    set pos=%s
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and child_id=%s
    returning 1
)
select exists (select 1 from move)
""", (123, 2, 1234, 123, 2, 0, 0, 123, 2, 1234)),
            FETCH_ONE,
            COMMIT])

    def test_move(self):
        #NOTE: only testing the single-node special case code here
        add_fetch_result([(1,)])
        add_fetch_result([(1,)])

        self.assertEqual(
                datahog.node.move(self.p, 1234, 2, 123, 124),
                True)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
with removal as (
    update edge
    set time_removed=now()
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and child_id=%s
    returning pos
), bump as (
    update edge
    set pos = pos - 1
    where
        exists (select 1 from removal)
        and time_removed is null
        and base_id=%s
        and ctx=%s
        and pos > (select pos from removal)
)
select 1 from removal
""", (123, 2, 1234, 123, 2)),
            ROWCOUNT,
            EXECUTE("""
insert into edge (base_id, ctx, child_id, pos)
select %s, %s, %s, (
    select count(*)
    from edge
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
)
where exists(
    select 1 from entity
    where
        time_removed is null
        and guid=%s
        and ctx=%s
)
""", (124, 2, 1234, 124, 2, 124, 1)),
            ROWCOUNT,
            COMMIT])

    def test_move_to_index(self):
        add_fetch_result([(1,)])
        add_fetch_result([(1,)])

        self.assertEqual(
                datahog.node.move(self.p, 1234, 2, 123, 124, 4),
                True)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
with removal as (
    update edge
    set time_removed=now()
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and child_id=%s
    returning pos
), bump as (
    update edge
    set pos = pos - 1
    where
        exists (select 1 from removal)
        and time_removed is null
        and base_id=%s
        and ctx=%s
        and pos > (select pos from removal)
)
select 1 from removal
""", (123, 2, 1234, 123, 2)),
            ROWCOUNT,
            EXECUTE("""
with bump as (
    update edge
    set pos=pos + 1
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and pos >= %s
        and exists (
            select 1 from entity
            where
                time_removed is null
                and guid=%s
                and ctx=%s
        )
)
insert into edge (base_id, ctx, child_id, pos)
select %s, %s, %s, %s
where exists (
    select 1 from entity
    where
        time_removed is null
        and guid=%s
        and ctx=%s
)
returning 1
""", (124, 2, 4, 124, 1, 124, 2, 1234, 4, 124, 1)),
            ROWCOUNT,
            COMMIT])

    def test_remove_lone_node(self):
        guid = 1234
        ctx = 2
        base_id = 123

        add_fetch_result([None])
        add_fetch_result([(guid,)])
        add_fetch_result([])
        add_fetch_result([])
        add_fetch_result([])
        add_fetch_result([])
        add_fetch_result([])

        self.assertEqual(
                datahog.node.remove(self.p, guid, ctx, base_id),
                True)

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
with removal as (
    update edge
    set time_removed=now()
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and child_id=%s
    returning pos
), bump as (
    update edge
    set pos = pos - 1
    where
        exists (select 1 from removal)
        and time_removed is null
        and base_id=%s
        and ctx=%s
        and pos > (select pos from removal)
)
select 1 from removal
""", (base_id, ctx, guid, base_id, ctx)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update node
set time_removed=now()
where
    time_removed is null
    and guid in (%s)
returning guid
""", (guid,)),
            FETCH_ALL,
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
        add_fetch_result([])

        guid = 1234
        ctx = 2
        base_id = 123

        self.assertEqual(
                datahog.node.remove(self.p, guid, ctx, base_id),
                False)

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
with removal as (
    update edge
    set time_removed=now()
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and child_id=%s
    returning pos
), bump as (
    update edge
    set pos = pos - 1
    where
        exists (select 1 from removal)
        and time_removed is null
        and base_id=%s
        and ctx=%s
        and pos > (select pos from removal)
)
select 1 from removal
""", (base_id, ctx, guid, base_id, ctx)),
            ROWCOUNT,
            TPC_ROLLBACK])


if __name__ == '__main__':
    unittest.main()

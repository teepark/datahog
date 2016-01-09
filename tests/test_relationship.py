# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import os
import sys
import unittest

import datahog
from datahog import error
import psycopg2

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import base
from pgmock import *


class RelationshipTests(base.TestCase):
    def setUp(self):
        super(RelationshipTests, self).setUp()
        datahog.set_context(1, datahog.NODE)
        datahog.set_context(2, datahog.NODE,
                {'base_ctx': 1, 'storage': datahog.storage.INT})
        datahog.set_context(3, datahog.RELATIONSHIP, {
            'base_ctx': 1, 'rel_ctx': 2})

    def test_create(self):
        add_fetch_result([(1,)])
        add_fetch_result([(1,)])

        self.assertEqual(
                datahog.relationship.create(self.p, 3, 123, 456),
                True)

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
insert into relationship (base_id, rel_id, ctx, forward, pos, flags)
select %s, %s, %s, %s, (
    select count(*)
    from relationship
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and forward=%s
), %s
where exists (
    select 1
    from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
)
returning 1
""", (123, 456, 3, True, 123, 3, True, 0, 123, 1)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
insert into relationship (base_id, rel_id, ctx, forward, pos, flags)
select %s, %s, %s, %s, (
    select count(*)
    from relationship
    where
        time_removed is null
        and rel_id=%s
        and ctx=%s
        and forward=%s
), %s
where exists (
    select 1
    from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
)
returning 1
""", (123, 456, 3, False, 456, 3, False, 0, 456, 2)),
            ROWCOUNT,
            COMMIT,
            TPC_COMMIT])

    def test_create_failure_noobject_forward(self):
        add_fetch_result([])

        self.assertRaises(error.NoObject,
                datahog.relationship.create, self.p, 3, 123, 456)

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
insert into relationship (base_id, rel_id, ctx, forward, pos, flags)
select %s, %s, %s, %s, (
    select count(*)
    from relationship
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and forward=%s
), %s
where exists (
    select 1
    from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
)
returning 1
""", (123, 456, 3, True, 123, 3, True, 0, 123, 1)),
            ROWCOUNT,
            TPC_ROLLBACK])

    def test_create_failure_noobject_reverse(self):
        add_fetch_result([(1,)])
        add_fetch_result([])

        self.assertRaises(error.NoObject,
                datahog.relationship.create, self.p, 3, 123, 456)

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
insert into relationship (base_id, rel_id, ctx, forward, pos, flags)
select %s, %s, %s, %s, (
    select count(*)
    from relationship
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and forward=%s
), %s
where exists (
    select 1
    from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
)
returning 1
""", (123, 456, 3, True, 123, 3, True, 0, 123, 1)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
insert into relationship (base_id, rel_id, ctx, forward, pos, flags)
select %s, %s, %s, %s, (
    select count(*)
    from relationship
    where
        time_removed is null
        and rel_id=%s
        and ctx=%s
        and forward=%s
), %s
where exists (
    select 1
    from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
)
returning 1
""", (123, 456, 3, False, 456, 3, False, 0, 456, 2)),
            ROWCOUNT,
            ROLLBACK,
            TPC_ROLLBACK])

    def test_create_failure_duplicate(self):
        query_fail(psycopg2.IntegrityError)

        self.assertEqual(
                datahog.relationship.create(self.p, 3, 123, 456),
                False)

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE_FAILURE("""
insert into relationship (base_id, rel_id, ctx, forward, pos, flags)
select %s, %s, %s, %s, (
    select count(*)
    from relationship
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and forward=%s
), %s
where exists (
    select 1
    from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
)
returning 1
""", (123, 456, 3, True, 123, 3, True, 0, 123, 1)),
            TPC_ROLLBACK])

    def test_create_with_positions(self):
        add_fetch_result([(1,)])
        add_fetch_result([(1,)])

        self.assertEqual(
                datahog.relationship.create(self.p, 3, 123, 456, 4, 5),
                True)

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
with eligible as (
    select 1
    from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
), bump as (
    update relationship
    set pos=pos + 1
    where
        exists (select 1 from eligible)
        and time_removed is null
        and forward=%s
        and base_id=%s
        and ctx=%s
        and pos >= %s
)
insert into relationship (base_id, rel_id, ctx, forward, pos, flags)
select %s, %s, %s, %s, %s, %s
where exists (select 1 from eligible)
returning 1
""", (123, 1, True, 123, 3, 4, 123, 456, 3, True, 4, 0)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
with eligible as (
    select 1
    from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
), bump as (
    update relationship
    set pos=pos + 1
    where
        exists (select 1 from eligible)
        and time_removed is null
        and forward=%s
        and rel_id=%s
        and ctx=%s
        and pos >= %s
)
insert into relationship (base_id, rel_id, ctx, forward, pos, flags)
select %s, %s, %s, %s, %s, %s
where exists (select 1 from eligible)
returning 1
""", (456, 2, False, 456, 3, 5, 123, 456, 3, False, 5, 0)),
            ROWCOUNT,
            COMMIT,
            TPC_COMMIT])

    def test_list_forwards(self):
        add_fetch_result([(456, 0, 0), (457, 0, 1), (458, 0, 2), (459, 0, 3)])

        self.assertEqual(
                datahog.relationship.list(self.p, 123, 3),
                ([
                    {'ctx': 3, 'base_id': 123, 'rel_id': 456, 'flags': set([])},
                    {'ctx': 3, 'base_id': 123, 'rel_id': 457, 'flags': set([])},
                    {'ctx': 3, 'base_id': 123, 'rel_id': 458, 'flags': set([])},
                    {'ctx': 3, 'base_id': 123, 'rel_id': 459, 'flags': set([])},
                ], 4))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select rel_id, flags, pos
from relationship
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and forward=%s
    and pos >= %s
order by pos asc
limit %s
""", (123, 3, True, 0, 100)),
            FETCH_ALL,
            COMMIT])

    def test_list_reverse(self):
        add_fetch_result([(123, 0, 0), (124, 0, 1), (125, 0, 2), (126, 0, 3)])

        self.assertEqual(
                datahog.relationship.list(self.p, 456, 3, False),
                ([
                    {'ctx': 3, 'base_id': 123, 'rel_id': 456, 'flags': set([])},
                    {'ctx': 3, 'base_id': 124, 'rel_id': 456, 'flags': set([])},
                    {'ctx': 3, 'base_id': 125, 'rel_id': 456, 'flags': set([])},
                    {'ctx': 3, 'base_id': 126, 'rel_id': 456, 'flags': set([])},
                ], 4))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags, pos
from relationship
where
    time_removed is null
    and rel_id=%s
    and ctx=%s
    and forward=%s
    and pos >= %s
order by pos asc
limit %s
""", (456, 3, False, 0, 100)),
            FETCH_ALL,
            COMMIT])

    def test_get_success(self):
        add_fetch_result([(456, 0, 7)])

        self.assertEqual(
                datahog.relationship.get(self.p, 3, 123, 456),
                {'ctx': 3, 'base_id': 123, 'rel_id': 456, 'flags': set([])})

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select rel_id, flags, pos
from relationship
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and forward=%s
    and pos >= %s
    and rel_id=%s
order by pos asc
limit %s
""", (123, 3, True, 0, 456, 1)),
            FETCH_ALL,
            COMMIT])

    def test_get_failure(self):
        add_fetch_result([])

        self.assertEqual(
                datahog.relationship.get(self.p, 3, 123, 456),
                None)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select rel_id, flags, pos
from relationship
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and forward=%s
    and pos >= %s
    and rel_id=%s
order by pos asc
limit %s
""", (123, 3, True, 0, 456, 1)),
            FETCH_ALL,
            COMMIT])

    def test_add_flags(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([(5,)])
        add_fetch_result([(5,)])

        self.assertEqual(
                datahog.relationship.set_flags(self.p, 123, 456, 3, [1, 3], []),
                set([1, 3]))

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update relationship
set flags=flags | %s
where time_removed is null and forward=%s and rel_id=%s and ctx=%s and base_id=%s
returning flags
""", (5, True, 456, 3, 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update relationship
set flags=flags | %s
where time_removed is null and forward=%s and rel_id=%s and ctx=%s and base_id=%s
returning flags
""", (5, False, 456, 3, 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_add_flags_no_rel(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([])

        self.assertEqual(
                datahog.relationship.set_flags(self.p, 123, 456, 3, [1, 3], []),
                None)

    def test_clear_flags(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([(2,)])
        add_fetch_result([(2,)])

        self.assertEqual(
                datahog.relationship.set_flags(self.p, 123, 456, 3, [], [1, 3]),
                set([2]))

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update relationship
set flags=flags & ~%s
where time_removed is null and forward=%s and rel_id=%s and ctx=%s and base_id=%s
returning flags
""", (5, True, 456, 3, 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update relationship
set flags=flags & ~%s
where time_removed is null and forward=%s and rel_id=%s and ctx=%s and base_id=%s
returning flags
""", (5, False, 456, 3, 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_clear_flags_no_rel(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([])

        self.assertEqual(
                datahog.relationship.set_flags(self.p, 123, 456, 3, [], [1, 3]),
                None)

    def test_set_flags_add(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([(5,)])
        add_fetch_result([(5,)])

        self.assertEqual(
                datahog.relationship.set_flags(self.p, 123, 456, 3, [1, 3], []),
                set([1, 3]))

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update relationship
set flags=flags | %s
where time_removed is null and forward=%s and rel_id=%s and ctx=%s and base_id=%s
returning flags
""", (5, True, 456, 3, 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update relationship
set flags=flags | %s
where time_removed is null and forward=%s and rel_id=%s and ctx=%s and base_id=%s
returning flags
""", (5, False, 456, 3, 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_set_flags_clear(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([(4,)])
        add_fetch_result([(4,)])

        self.assertEqual(
                datahog.relationship.set_flags(self.p, 123, 456, 3, [], [1, 2]),
                set([3]))

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update relationship
set flags=flags & ~%s
where time_removed is null and forward=%s and rel_id=%s and ctx=%s and base_id=%s
returning flags
""", (3, True, 456, 3, 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update relationship
set flags=flags & ~%s
where time_removed is null and forward=%s and rel_id=%s and ctx=%s and base_id=%s
returning flags
""", (3, False, 456, 3, 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_set_flags_both(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([(5,)])
        add_fetch_result([(5,)])

        self.assertEqual(
                datahog.relationship.set_flags(self.p, 123, 456, 3, [1, 3], [2]),
                set([1, 3]))

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update relationship
set flags=(flags & ~%s) | %s
where time_removed is null and forward=%s and rel_id=%s and ctx=%s and base_id=%s
returning flags
""", (2, 5, True, 456, 3, 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update relationship
set flags=(flags & ~%s) | %s
where time_removed is null and forward=%s and rel_id=%s and ctx=%s and base_id=%s
returning flags
""", (2, 5, False, 456, 3, 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_shift(self):
        add_fetch_result([(True,)])

        self.assertEqual(
                datahog.relationship.shift(self.p, 123, 456, 3, True, 7),
                True)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
with oldpos as (
    select pos
    from relationship
    where
        time_removed is null
        and forward=%s
        and base_id=%s
        and ctx=%s
        and rel_id=%s
), bump as (
    update relationship
    set pos=pos + (case
        when (select pos from oldpos) < pos
        then -1
        else 1
        end)
    where
        exists (select 1 from oldpos)
        and time_removed is null
        and forward=%s
        and base_id=%s
        and ctx=%s
        and pos between symmetric (select pos from oldpos) and %s
    returning 1
), move as (
    update relationship
    set pos=%s
    where
        time_removed is null
        and forward=%s
        and base_id=%s
        and ctx=%s
        and rel_id=%s
    returning 1
)
select exists (select 1 from move)
""", (True, 123, 3, 456, True, 123, 3, 7, 7, True, 123, 3, 456)),
            FETCH_ONE,
            COMMIT])

    def test_shift_failure(self):
        add_fetch_result([(False,)])

        self.assertEqual(
                datahog.relationship.shift(self.p, 123, 456, 3, True, 7),
                False)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
with oldpos as (
    select pos
    from relationship
    where
        time_removed is null
        and forward=%s
        and base_id=%s
        and ctx=%s
        and rel_id=%s
), bump as (
    update relationship
    set pos=pos + (case
        when (select pos from oldpos) < pos
        then -1
        else 1
        end)
    where
        exists (select 1 from oldpos)
        and time_removed is null
        and forward=%s
        and base_id=%s
        and ctx=%s
        and pos between symmetric (select pos from oldpos) and %s
    returning 1
), move as (
    update relationship
    set pos=%s
    where
        time_removed is null
        and forward=%s
        and base_id=%s
        and ctx=%s
        and rel_id=%s
    returning 1
)
select exists (select 1 from move)
""", (True, 123, 3, 456, True, 123, 3, 7, 7, True, 123, 3, 456)),
            FETCH_ONE,
            COMMIT])

    def test_remove(self):
        add_fetch_result([(1,)])
        add_fetch_result([(1,)])

        self.assertEqual(
                datahog.relationship.remove(self.p, 123, 456, 3),
                True)

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
with removal as (
    update relationship
    set time_removed=now()
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and forward=%s
        and rel_id=%s
    returning pos
), bump as (
    update relationship
    set pos = pos - 1
    where
        exists (select 1 from removal)
        and time_removed is null
        and base_id=%s
        and ctx=%s
        and forward=%s
        and pos > (select pos from removal)
)
select 1 from removal
""", (123, 3, True, 456, 123, 3, True)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
with removal as (
    update relationship
    set time_removed=now()
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and forward=%s
        and rel_id=%s
    returning pos
), bump as (
    update relationship
    set pos = pos - 1
    where
        exists (select 1 from removal)
        and time_removed is null
        and rel_id=%s
        and ctx=%s
        and forward=%s
        and pos > (select pos from removal)
)
select 1 from removal
""", (123, 3, False, 456, 456, 3, False)),
            ROWCOUNT,
            COMMIT,
            TPC_COMMIT])

    def test_remove_failure_forward(self):
        add_fetch_result([])

        self.assertEqual(
                datahog.relationship.remove(self.p, 123, 456, 3),
                False)

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
with removal as (
    update relationship
    set time_removed=now()
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and forward=%s
        and rel_id=%s
    returning pos
), bump as (
    update relationship
    set pos = pos - 1
    where
        exists (select 1 from removal)
        and time_removed is null
        and base_id=%s
        and ctx=%s
        and forward=%s
        and pos > (select pos from removal)
)
select 1 from removal
""", (123, 3, True, 456, 123, 3, True)),
            ROWCOUNT,
            TPC_ROLLBACK])

    def test_remove_failure_reverse(self):
        add_fetch_result([(1,)])
        add_fetch_result([])

        self.assertEqual(
                datahog.relationship.remove(self.p, 123, 456, 3),
                False)

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
with removal as (
    update relationship
    set time_removed=now()
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and forward=%s
        and rel_id=%s
    returning pos
), bump as (
    update relationship
    set pos = pos - 1
    where
        exists (select 1 from removal)
        and time_removed is null
        and base_id=%s
        and ctx=%s
        and forward=%s
        and pos > (select pos from removal)
)
select 1 from removal
""", (123, 3, True, 456, 123, 3, True)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
with removal as (
    update relationship
    set time_removed=now()
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and forward=%s
        and rel_id=%s
    returning pos
), bump as (
    update relationship
    set pos = pos - 1
    where
        exists (select 1 from removal)
        and time_removed is null
        and rel_id=%s
        and ctx=%s
        and forward=%s
        and pos > (select pos from removal)
)
select 1 from removal
""", (123, 3, False, 456, 456, 3, False)),
            ROWCOUNT,
            ROLLBACK,
            TPC_ROLLBACK])


if __name__ == '__main__':
    unittest.main()

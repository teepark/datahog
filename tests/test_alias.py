# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import hashlib
import hmac
import os
import sys
import unittest

import datahog
from datahog import error
import psycopg2

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import base
from pgmock import *


class AliasTests(base.TestCase):
    def setUp(self):
        super(AliasTests, self).setUp()
        datahog.set_context(1, datahog.NODE)
        datahog.set_context(2, datahog.ALIAS, {'base_ctx': 1})

    def test_set(self):
        add_fetch_result([])
        add_fetch_result([None])

        self.assertEqual(
                datahog.alias.set(self.p, 123, 2, 'value'),
                True)

        h = hmac.new(self.p.digestkey, 'value', hashlib.sha1).digest()

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
with selectquery (base_id) as (
    select base_id
    from alias_lookup
    where
        time_removed is null
        and hash=%s
        and ctx=%s
),
insertquery as (
    insert into alias_lookup (hash, ctx, base_id, flags)
    select %s, %s, %s, %s
    where not exists (select 1 from selectquery)
)
select base_id
from selectquery
""", (h, 2, h, 2, 123, 0)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
insert into alias (base_id, ctx, value, pos, flags)
select %s, %s, %s, coalesce((
    select pos + 1
    from alias
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
    order by pos desc
    limit 1
), 1), %s
where exists (
    select 1 from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
)
""", (123, 2, 'value', 123, 2, 0, 123, 1)),
            ROWCOUNT,
            COMMIT,
            TPC_COMMIT])

    def test_set_failure_already_exists(self):
        add_fetch_result([(123,)])

        self.assertEqual(
                datahog.alias.set(self.p, 123, 2, 'value'),
                False)

        h = hmac.new(self.p.digestkey, 'value', hashlib.sha1).digest()

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
with selectquery (base_id) as (
    select base_id
    from alias_lookup
    where
        time_removed is null
        and hash=%s
        and ctx=%s
),
insertquery as (
    insert into alias_lookup (hash, ctx, base_id, flags)
    select %s, %s, %s, %s
    where not exists (select 1 from selectquery)
)
select base_id
from selectquery
""", (h, 2, h, 2, 123, 0)),
            ROWCOUNT,
            FETCH_ONE,
            TPC_ROLLBACK])

    def test_set_failure_claimed(self):
        add_fetch_result([(124,)])

        self.assertRaises(error.AliasInUse,
                datahog.alias.set, self.p, 123, 2, 'value')

        h = hmac.new(self.p.digestkey, 'value', hashlib.sha1).digest()

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
with selectquery (base_id) as (
    select base_id
    from alias_lookup
    where
        time_removed is null
        and hash=%s
        and ctx=%s
),
insertquery as (
    insert into alias_lookup (hash, ctx, base_id, flags)
    select %s, %s, %s, %s
    where not exists (select 1 from selectquery)
)
select base_id
from selectquery
""", (h, 2, h, 2, 123, 0)),
            ROWCOUNT,
            FETCH_ONE,
            TPC_ROLLBACK])

    def test_set_race_condition_fallback(self):
        @query_fail
        def qf():
            query_fail(None)
            return psycopg2.IntegrityError()

        add_fetch_result([(123, 0)])

        self.assertEqual(
                datahog.alias.set(self.p, 123, 2, 'value'),
                False)

        h = hmac.new(self.p.digestkey, 'value', hashlib.sha1).digest()

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE_FAILURE("""
with selectquery (base_id) as (
    select base_id
    from alias_lookup
    where
        time_removed is null
        and hash=%s
        and ctx=%s
),
insertquery as (
    insert into alias_lookup (hash, ctx, base_id, flags)
    select %s, %s, %s, %s
    where not exists (select 1 from selectquery)
)
select base_id
from selectquery
""", (h, 2, h, 2, 123, 0)),
            TPC_ROLLBACK,
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from alias_lookup
where
    time_removed is null
    and hash=%s
    and ctx=%s
""", (h, 2)),
            ROWCOUNT,
            FETCH_ONE,
            ROLLBACK])

    def test_lookup(self):
        add_fetch_result([(123, 0)])

        self.assertEqual(
                datahog.alias.lookup(self.p, 'value', 2),
                {'base_id': 123, 'ctx': 2, 'value': 'value', 'flags': set([])})

        h = hmac.new(self.p.digestkey, 'value', hashlib.sha1).digest()

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from alias_lookup
where
    time_removed is null
    and hash=%s
    and ctx=%s
""", (h, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT])

    def test_lookup_failure(self):
        add_fetch_result([])

        self.assertEqual(
                datahog.alias.lookup(self.p, 'value', 2),
                None)

        h = hmac.new(self.p.digestkey, 'value', hashlib.sha1).digest()

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from alias_lookup
where
    time_removed is null
    and hash=%s
    and ctx=%s
""", (h, 2)),
            ROWCOUNT,
            COMMIT])

    def test_list(self):
        add_fetch_result([(0, 'val1', 0), (0, 'val2', 1), (0, 'val3', 2)])

        self.assertEqual(
                datahog.alias.list(self.p, 123, 2),
                ([
                    {'base_id': 123, 'ctx': 2, 'value': 'val1',
                        'flags': set([])},
                    {'base_id': 123, 'ctx': 2, 'value': 'val2',
                        'flags': set([])},
                    {'base_id': 123, 'ctx': 2, 'value': 'val3',
                        'flags': set([])},
                ], 3))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select flags, value, pos
from alias
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and pos >= %s
order by pos asc
limit %s
""", (123, 2, 0, 100)),
            FETCH_ALL,
            COMMIT])

    def test_list_empty(self):
        add_fetch_result([])

        self.assertEqual(
                datahog.alias.list(self.p, 123, 2),
                ([], 0))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select flags, value, pos
from alias
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and pos >= %s
order by pos asc
limit %s
""", (123, 2, 0, 100)),
            FETCH_ALL,
            COMMIT])

    def test_batch(self):
        add_fetch_result([
            (123, 0, 2, 'val1'),
            (124, 0, 2, 'val2'),
            (126, 0, 2, 'val3')])

        self.assertEqual(
                datahog.alias.batch(self.p,
                    [(123, 2), (124, 2), (125, 2), (126, 2)]),
                [
                    {'base_id': 123, 'flags': set([]), 'ctx': 2,
                        'value': 'val1'},
                    {'base_id': 124, 'flags': set([]), 'ctx': 2,
                        'value': 'val2'},
                    None,
                    {'base_id': 126, 'flags': set([]), 'ctx': 2,
                        'value': 'val3'}])

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
with window_query as (
    select base_id, flags, ctx, value, rank() over (
        partition by base_id, ctx
        order by pos
    ) as r
    from alias
    where
        time_removed is null
        and (base_id, ctx) in ((%s, %s),(%s, %s),(%s, %s),(%s, %s))
)
select base_id, flags, ctx, value
from window_query
where r=1
""", (123, 2, 124, 2, 125, 2, 126, 2)),
            FETCH_ALL,
            COMMIT])

    def test_add_flags(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(123, 5)])
        add_fetch_result([(5,)])
        add_fetch_result([(5,)])

        self.assertEqual(
                datahog.alias.set_flags(self.p, 123, 2, 'value', [1, 3], []),
                set([1, 3]))

        h = hmac.new(self.p.digestkey, 'value', hashlib.sha1).digest()

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from alias_lookup
where
    time_removed is null
    and hash=%s
    and ctx=%s
""", (h, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update alias_lookup
set flags=flags | %s
where time_removed is null and ctx=%s and hash=%s
returning flags
""", (5, 2, h)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update alias
set flags=flags | %s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (5, 2, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_add_flags_no_alias(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(123, 5)])
        add_fetch_result([])

        self.assertEqual(
                datahog.alias.set_flags(self.p, 123, 2, 'value', [1, 3], []),
                None)

    def test_clear_flags(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(123, 5)])
        add_fetch_result([(1,)])
        add_fetch_result([(1,)])

        self.assertEqual(
                datahog.alias.set_flags(self.p, 123, 2, 'value', [], [2, 3]),
                set([1]))

        h = hmac.new(self.p.digestkey, 'value', hashlib.sha1).digest()

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from alias_lookup
where
    time_removed is null
    and hash=%s
    and ctx=%s
""", (h, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update alias_lookup
set flags=flags & ~%s
where time_removed is null and ctx=%s and hash=%s
returning flags
""", (6, 2, h)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update alias
set flags=flags & ~%s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (6, 2, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_clear_flags_no_alias(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(123, 5)])
        add_fetch_result([])

        self.assertEqual(
                datahog.alias.set_flags(self.p, 123, 2, 'value', [], [1, 3]),
                None)

    def test_set_flags_add(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(123, 5)])
        add_fetch_result([(5,)])
        add_fetch_result([(5,)])

        self.assertEqual(
                datahog.alias.set_flags(self.p, 123, 2, 'value', [1, 3], []),
                set([1, 3]))

        h = hmac.new(self.p.digestkey, 'value', hashlib.sha1).digest()

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from alias_lookup
where
    time_removed is null
    and hash=%s
    and ctx=%s
""", (h, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update alias_lookup
set flags=flags | %s
where time_removed is null and ctx=%s and hash=%s
returning flags
""", (5, 2, h)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update alias
set flags=flags | %s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (5, 2, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_set_flags_clear(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(123, 5)])
        add_fetch_result([(4,)])
        add_fetch_result([(4,)])

        self.assertEqual(
                datahog.alias.set_flags(self.p, 123, 2, 'value', [], [1, 2]),
                set([3]))

        h = hmac.new(self.p.digestkey, 'value', hashlib.sha1).digest()

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from alias_lookup
where
    time_removed is null
    and hash=%s
    and ctx=%s
""", (h, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update alias_lookup
set flags=flags & ~%s
where time_removed is null and ctx=%s and hash=%s
returning flags
""", (3, 2, h)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update alias
set flags=flags & ~%s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (3, 2, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_set_flags_both(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(123, 5)])
        add_fetch_result([(5,)])
        add_fetch_result([(5,)])

        self.assertEqual(
                datahog.alias.set_flags(self.p, 123, 2, 'value', [1, 3], [2]),
                set([1, 3]))

        h = hmac.new(self.p.digestkey, 'value', hashlib.sha1).digest()

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from alias_lookup
where
    time_removed is null
    and hash=%s
    and ctx=%s
""", (h, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update alias_lookup
set flags=(flags & ~%s) | %s
where time_removed is null and ctx=%s and hash=%s
returning flags
""", (2, 5, 2, h)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update alias
set flags=(flags & ~%s) | %s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (2, 5, 2, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_set_flags_no_alias(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(123, 5)])
        add_fetch_result([])

        self.assertEqual(
                datahog.alias.set_flags(self.p, 123, 2, 'value', [], [1, 2]),
                None)
            
    def test_shift(self):
        add_fetch_result([(True,)])

        self.assertEqual(
                datahog.alias.shift(self.p, 123, 2, 'value', 3),
                True)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
with oldpos as (
    select pos
    from alias
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and value=%s
), bump as (
    update alias
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
), maxpos(n) as (
    select pos
    from alias
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
    order by pos desc
    limit 1
), move as (
    update alias
    set pos=(case
        when %s > (select n from maxpos)
        then (select n from maxpos)
        else %s
        end)
    where
        exists (select 1 from oldpos)
        and time_removed is null
        and base_id=%s
        and ctx=%s
        and value=%s
    returning 1
)
select exists (select 1 from move)
""", (123, 2, 'value', 123, 2, 3, 123, 2, 3, 3, 123, 2, 'value')),
            FETCH_ONE,
            COMMIT])

    def test_remove(self):
        add_fetch_result([(123, 0)])
        add_fetch_result([()])
        add_fetch_result([()])

        self.assertEqual(
                datahog.alias.remove(self.p, 123, 2, 'value'),
                True)

        h = hmac.new(self.p.digestkey, 'value', hashlib.sha1).digest()

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from alias_lookup
where
    time_removed is null
    and hash=%s
    and ctx=%s
""", (h, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update alias_lookup
set time_removed=now()
where
    time_removed is null
    and hash=%s
    and ctx=%s
    and base_id=%s
""", (h, 2, 123)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
with removal as (
    update alias
    set time_removed=now()
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and value=%s
    returning pos
), bump as (
    update alias
    set pos = pos - 1
    where
        exists (select 1 from removal)
        and time_removed is null
        and base_id=%s
        and ctx=%s
        and pos > (select pos from removal)
)
select 1 from removal
""", (123, 2, 'value', 123, 2)),
            ROWCOUNT,
            COMMIT,
            TPC_COMMIT])


if __name__ == '__main__':
    unittest.main()

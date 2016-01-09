# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import os
import sys
import unittest

import datahog
from datahog import error
import fuzzy
import psycopg2

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import base
from pgmock import *


def _dm(full):
    dm, dmalt = fuzzy.DMetaphone()(full)
    dm = dm.ljust(4, ' ')
    if dmalt is not None:
        dmalt = dmalt.ljust(4, ' ')
    return dm, dmalt


class NameTests(base.TestCase):
    def setUp(self):
        super(NameTests, self).setUp()
        datahog.set_context(1, datahog.NODE)
        datahog.set_context(2, datahog.NAME,
                {'base_ctx': 1, 'search': datahog.search.PHONETIC,
                    'phonetic_loose': True})
        datahog.set_context(3, datahog.NAME,
                {'base_ctx': 1, 'search': datahog.search.PREFIX})

    def test_create_phonetic(self):
        add_fetch_result([None])

        self.assertEqual(
                datahog.name.create(self.p, 123, 2, 'value'),
                True)

        dm, dmalt = _dm('value')

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
insert into name (base_id, ctx, value, flags, pos)
select %s, %s, %s, %s, coalesce((
    select pos + 1
    from name
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
    order by pos desc
    limit 1
), 1)
where exists (
    select 1 from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
)
""", (123, 2, 'value', 0, 123, 2, 123, 1)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
insert into phonetic_lookup (value, code, flags, ctx, base_id)
values (%s, %s, %s, %s, %s)
""", ('value', dm, 0, 2, 123)),
            TPC_PREPARE,
            RESET,
            TPC_COMMIT,
            TPC_COMMIT])

    def test_create_phonetic_two_codes(self):
        add_fetch_result([None])

        self.assertEqual(
                datahog.name.create(self.p, 123, 2, 'window'),
                True)

        dm, dmalt = _dm('window')

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
insert into name (base_id, ctx, value, flags, pos)
select %s, %s, %s, %s, coalesce((
    select pos + 1
    from name
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
    order by pos desc
    limit 1
), 1)
where exists (
    select 1 from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
)
""", (123, 2, 'window', 0, 123, 2, 123, 1)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
insert into phonetic_lookup (value, code, flags, ctx, base_id)
values (%s, %s, %s, %s, %s)
""", ('window', dm, 0, 2, 123)),
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
insert into phonetic_lookup (value, code, flags, ctx, base_id)
values (%s, %s, %s, %s, %s)
""", ('window', dmalt, 0, 2, 123)),
            COMMIT,
            TPC_COMMIT,
            TPC_COMMIT])

    def test_create_prefix(self):
        add_fetch_result([None])

        self.assertEqual(
                datahog.name.create(self.p, 123, 3, 'value'),
                True)

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
insert into name (base_id, ctx, value, flags, pos)
select %s, %s, %s, %s, coalesce((
    select pos + 1
    from name
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
    order by pos desc
    limit 1
), 1)
where exists (
    select 1 from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
)
""", (123, 3, 'value', 0, 123, 3, 123, 1)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
insert into prefix_lookup (value, flags, ctx, base_id)
values (%s, %s, %s, %s)
""", ('value', 0, 3, 123)),
            COMMIT,
            TPC_COMMIT])

    def test_create_failure(self):
        add_fetch_result([])

        self.assertEqual(
                datahog.name.create(self.p, 123, 2, 'value'),
                False)

        self.assertEqual(eventlog, [
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
insert into name (base_id, ctx, value, flags, pos)
select %s, %s, %s, %s, coalesce((
    select pos + 1
    from name
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
    order by pos desc
    limit 1
), 1)
where exists (
    select 1 from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
)
""", (123, 2, 'value', 0, 123, 2, 123, 1)),
            ROWCOUNT,
            TPC_ROLLBACK])

    def test_search_prefix(self):
        add_fetch_result([(123, 0, 'value1'), (124, 0, 'value2')])

        self.assertEqual(
                datahog.name.search(self.p, 'value', 3),
                ([
                    {'base_id': 123, 'ctx': 3, 'value': 'value1',
                        'flags': set([])},
                    {'base_id': 124, 'ctx': 3, 'value': 'value2',
                        'flags': set([])},
                ], 'value2'))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags, value
from prefix_lookup
where
    time_removed is null
    and ctx=%s
    and value like %s || '%%'
    and value > %s
order by value
limit %s
""", (3, 'value', '', 100)),
            FETCH_ALL,
            COMMIT])

    def test_search_phonetic(self):
        add_fetch_result([
            (123, 0, 'fancy'),
            (124, 0, 'funk'),
            (125, 0, 'phancy')])

        dm, dmalt = _dm('fancy')

        self.assertEqual(
                datahog.name.search(self.p, 'fancy', 2),
                ([
                    {'base_id': 123, 'ctx': 2, 'value': 'fancy',
                        'flags': set([])},
                    {'base_id': 124, 'ctx': 2, 'value': 'funk',
                        'flags': set([])},
                    {'base_id': 125, 'ctx': 2, 'value': 'phancy',
                        'flags': set([])},
                ], {dm: 125}))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags, value
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and base_id > %s
order by base_id
limit %s
""", (2, dm, 0, 100)),
            FETCH_ALL,
            COMMIT])

    def test_search_phonetic_page_2(self):
        add_fetch_result([
            (126, 0, 'fancy'),
            (127, 0, 'funk'),
            (128, 0, 'phancy')])

        dm, dmalt = _dm('fancy')

        self.assertEqual(
                datahog.name.search(self.p, 'fancy', 2, start={dm: 125}),
                ([
                    {'base_id': 126, 'ctx': 2, 'value': 'fancy',
                        'flags': set([])},
                    {'base_id': 127, 'ctx': 2, 'value': 'funk',
                        'flags': set([])},
                    {'base_id': 128, 'ctx': 2, 'value': 'phancy',
                        'flags': set([])},
                ], {dm: 128}))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags, value
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and base_id > %s
order by base_id
limit %s
""", (2, dm, 125, 100)),
            FETCH_ALL,
            COMMIT])

    def test_search_phonetic_both(self):
        # not the greatest results, but they would match
        add_fetch_result([(126, 0, 'ant')])
        add_fetch_result([(127, 0, 'fntf')])

        dm, dmalt = _dm('window')

        self.assertEqual(
                datahog.name.search(self.p, 'window', 2),
                ([
                    {'base_id': 126, 'ctx': 2, 'value': 'ant',
                        'flags': set([])},
                    {'base_id': 127, 'ctx': 2, 'value': 'fntf',
                        'flags': set([])},
                ], {dm: 126, dmalt: 127}))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags, value
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and base_id > %s
order by base_id
limit %s
""", (2, dm, 0, 100)),
            FETCH_ALL,
            COMMIT,
            GET_CURSOR,
            EXECUTE("""
select base_id, flags, value
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and base_id > %s
order by base_id
limit %s
""", (2, dmalt, 0, 100)),
            FETCH_ALL,
            COMMIT])

    def test_list(self):
        add_fetch_result([
            (0, 'foo', 0),
            (0, 'bar', 1),
            (0, 'baz', 2)])

        self.assertEqual(
                datahog.name.list(self.p, 123, 2),
                ([
                    {'base_id': 123, 'ctx': 2, 'flags': set([]),
                        'value': 'foo'},
                    {'base_id': 123, 'ctx': 2, 'flags': set([]),
                        'value': 'bar'},
                    {'base_id': 123, 'ctx': 2, 'flags': set([]),
                        'value': 'baz'},
                ], 3))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select flags, value, pos
from name
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

    def test_add_flags_prefix(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([(123, 0)])
        add_fetch_result([(6,)])
        add_fetch_result([(6,)])

        self.assertEqual(
                datahog.name.set_flags(self.p, 123, 3, 'value', [2, 3], []),
                set([2, 3]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from prefix_lookup
where
    time_removed is null
    and ctx=%s
    and value=%s
    and base_id=%s
""", (3, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update name
set flags=flags | %s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (6, 3, 'value', 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update prefix_lookup
set flags=flags | %s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (6, 3, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_add_flags_phonetic_one(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(123, 0)])
        add_fetch_result([(6,)])
        add_fetch_result([(6,)])

        dm, dmalt = _dm('value')

        self.assertEqual(
                datahog.name.set_flags(self.p, 123, 2, 'value', [2, 3], []),
                set([2, 3]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select 1
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dm, 'value', 123)),
            ROWCOUNT,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update name
set flags=flags | %s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (6, 2, 'value', 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update phonetic_lookup
set flags=flags | %s
where time_removed is null and code=%s and ctx=%s and base_id=%s and value=%s
returning flags
""", (6, dm, 2, 123, 'value')),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_add_flags_phonetic_two(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(123, 0)])
        add_fetch_result([(123, 0)])
        add_fetch_result([(6,)])
        add_fetch_result([(6,)])
        add_fetch_result([(6,)])

        dm, dmalt = _dm('window')

        self.assertEqual(
                datahog.name.set_flags(self.p, 123, 2, 'window', [2, 3], []),
                set([2, 3]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select 1
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dm, 'window', 123)),
            ROWCOUNT,
            COMMIT,
            GET_CURSOR,
            EXECUTE("""
select 1
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dmalt, 'window', 123)),
            ROWCOUNT,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update name
set flags=flags | %s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (6, 2, 'window', 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update phonetic_lookup
set flags=flags | %s
where time_removed is null and code=%s and ctx=%s and base_id=%s and value=%s
returning flags
""", (6, dm, 2, 123, 'window')),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update phonetic_lookup
set flags=flags | %s
where time_removed is null and code=%s and ctx=%s and base_id=%s and value=%s
returning flags
""", (6, dmalt, 2, 123, 'window')),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT,
            TPC_COMMIT])

    def test_add_flags_no_name(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([(123, 0)])
        add_fetch_result([])

        self.assertEqual(
                datahog.name.set_flags(self.p, 123, 3, 'value', [2, 3], []),
                None)

    def test_clear_flags_prefix(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([(123, 0)])
        add_fetch_result([(1,)])
        add_fetch_result([(1,)])

        self.assertEqual(
                datahog.name.set_flags(self.p, 123, 3, 'value', [], [2, 3]),
                set([1]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from prefix_lookup
where
    time_removed is null
    and ctx=%s
    and value=%s
    and base_id=%s
""", (3, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update name
set flags=flags & ~%s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (6, 3, 'value', 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update prefix_lookup
set flags=flags & ~%s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (6, 3, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_clear_flags_phonetic_one(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(123, 0)])
        add_fetch_result([(1,)])
        add_fetch_result([(1,)])

        dm, dmalt = _dm('value')

        self.assertEqual(
                datahog.name.set_flags(self.p, 123, 2, 'value', [], [2, 3]),
                set([1]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select 1
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dm, 'value', 123)),
            ROWCOUNT,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update name
set flags=flags & ~%s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (6, 2, 'value', 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update phonetic_lookup
set flags=flags & ~%s
where time_removed is null and code=%s and ctx=%s and base_id=%s and value=%s
returning flags
""", (6, dm, 2, 123, 'value')),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_clear_flags_phonetic_two(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(123, 0)])
        add_fetch_result([(123, 0)])
        add_fetch_result([(1,)])
        add_fetch_result([(1,)])
        add_fetch_result([(1,)])

        dm, dmalt = _dm('window')

        self.assertEqual(
                datahog.name.set_flags(self.p, 123, 2, 'window', [], [2, 3]),
                set([1]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select 1
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dm, 'window', 123)),
            ROWCOUNT,
            COMMIT,
            GET_CURSOR,
            EXECUTE("""
select 1
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dmalt, 'window', 123)),
            ROWCOUNT,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update name
set flags=flags & ~%s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (6, 2, 'window', 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update phonetic_lookup
set flags=flags & ~%s
where time_removed is null and code=%s and ctx=%s and base_id=%s and value=%s
returning flags
""", (6, dm, 2, 123, 'window')),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update phonetic_lookup
set flags=flags & ~%s
where time_removed is null and code=%s and ctx=%s and base_id=%s and value=%s
returning flags
""", (6, dmalt, 2, 123, 'window')),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT,
            TPC_COMMIT])

    def test_clear_flags_no_name(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([(123, 0)])
        add_fetch_result([])

        self.assertEqual(
                datahog.name.set_flags(self.p, 123, 3, 'value', [], [2, 3]),
                None)

    def test_set_flags_add(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([(123, 0)])
        add_fetch_result([(5,)])
        add_fetch_result([(5,)])

        self.assertEqual(
                datahog.name.set_flags(self.p, 123, 3, 'value', [1, 3], []),
                set([1, 3]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from prefix_lookup
where
    time_removed is null
    and ctx=%s
    and value=%s
    and base_id=%s
""", (3, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update name
set flags=flags | %s
where
    time_removed is null
    and ctx=%s
    and value=%s
    and base_id=%s
returning flags
""", (5, 3, 'value', 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update prefix_lookup
set flags=flags | %s
where
    time_removed is null
    and ctx=%s
    and value=%s
    and base_id=%s
returning flags
""", (5, 3, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_set_flags_clear(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([(123, 0)])
        add_fetch_result([(4,)])
        add_fetch_result([(4,)])

        self.assertEqual(
                datahog.name.set_flags(self.p, 123, 3, 'value', [], [1, 2]),
                set([3]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from prefix_lookup
where
    time_removed is null
    and ctx=%s
    and value=%s
    and base_id=%s
""", (3, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update name
set flags=flags & ~%s
where
    time_removed is null
    and ctx=%s
    and value=%s
    and base_id=%s
returning flags
""", (3, 3, 'value', 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update prefix_lookup
set flags=flags & ~%s
where
    time_removed is null
    and ctx=%s
    and value=%s
    and base_id=%s
returning flags
""", (3, 3, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_set_flags_both(self):
        datahog.set_flag(1, 3)
        datahog.set_flag(2, 3)
        datahog.set_flag(3, 3)

        add_fetch_result([(123, 0)])
        add_fetch_result([(5,)])
        add_fetch_result([(5,)])

        self.assertEqual(
                datahog.name.set_flags(self.p, 123, 3, 'value', [1, 3], [2]),
                set([1, 3]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from prefix_lookup
where
    time_removed is null
    and ctx=%s
    and value=%s
    and base_id=%s
""", (3, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update name
set flags=(flags & ~%s) | %s
where
    time_removed is null
    and ctx=%s
    and value=%s
    and base_id=%s
returning flags
""", (2, 5, 3, 'value', 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update prefix_lookup
set flags=(flags & ~%s) | %s
where
    time_removed is null
    and ctx=%s
    and value=%s
    and base_id=%s
returning flags
""", (2, 5, 3, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT])

    def test_set_flags_phonetic_both(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(123, 0)])
        add_fetch_result([(123, 0)])
        add_fetch_result([(6,)])
        add_fetch_result([(6,)])
        add_fetch_result([(6,)])

        dm, dmalt = _dm('window')

        self.assertEqual(
                datahog.name.set_flags(self.p, 123, 2, 'window', [2, 3], [1]),
                set([2, 3]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select 1
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dm, 'window', 123)),
            ROWCOUNT,
            COMMIT,
            GET_CURSOR,
            EXECUTE("""
select 1
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dmalt, 'window', 123)),
            ROWCOUNT,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update name
set flags=(flags & ~%s) | %s
where time_removed is null and ctx=%s and value=%s and base_id=%s
returning flags
""", (1, 6, 2, 'window', 123)),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update phonetic_lookup
set flags=(flags & ~%s) | %s
where time_removed is null and code=%s and ctx=%s and base_id=%s and value=%s
returning flags
""", (1, 6, dm, 2, 123, 'window')),
            FETCH_ALL,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update phonetic_lookup
set flags=(flags & ~%s) | %s
where time_removed is null and code=%s and ctx=%s and base_id=%s and value=%s
returning flags
""", (1, 6, dmalt, 2, 123, 'window')),
            FETCH_ALL,
            COMMIT,
            TPC_COMMIT,
            TPC_COMMIT])

    def test_shift(self):
        add_fetch_result([None])

        self.assertEqual(
                datahog.name.shift(self.p, 123, 2, 'value', 7),
                True)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
with oldpos as (
    select pos
    from name
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and value=%s
), bump as (
    update name
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
    from name
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
    order by pos desc
    limit 1
), move as (
    update name
    set pos=(case
        when %s > (select n from maxpos)
        then (select n from maxpos)
        else %s
        end)
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and value=%s
    returning 1
)
select 1 from move
""", (123, 2, 'value', 123, 2, 7, 123, 2, 7, 7, 123, 2, 'value')),
            ROWCOUNT,
            COMMIT])

    def test_shift_failure(self):
        add_fetch_result([])

        self.assertEqual(
                datahog.name.shift(self.p, 123, 2, 'value', 7),
                False)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
with oldpos as (
    select pos
    from name
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and value=%s
), bump as (
    update name
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
    from name
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
    order by pos desc
    limit 1
), move as (
    update name
    set pos=(case
        when %s > (select n from maxpos)
        then (select n from maxpos)
        else %s
        end)
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and value=%s
    returning 1
)
select 1 from move
""", (123, 2, 'value', 123, 2, 7, 123, 2, 7, 7, 123, 2, 'value')),
            ROWCOUNT,
            ROLLBACK])

    def test_remove_prefix(self):
        add_fetch_result([(123, 0)])
        add_fetch_result([(1,)])
        add_fetch_result([None])

        self.assertEqual(
                datahog.name.remove(self.p, 123, 3, 'value'),
                True)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select base_id, flags
from prefix_lookup
where
    time_removed is null
    and ctx=%s
    and value=%s
    and base_id=%s
""", (3, 'value', 123)),
            FETCH_ALL,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
with removal as (
    update name
    set time_removed=now()
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and value=%s
    returning pos
), bump as (
    update name
    set pos = pos - 1
    where
        exists (select 1 from removal)
        and time_removed is null
        and base_id=%s
        and ctx=%s
        and pos > (select pos from removal)
)
select 1 from removal
""", (123, 3, 'value', 123, 3)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update prefix_lookup
set time_removed=now()
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and value=%s
""", (123, 3, 'value')),
            ROWCOUNT,
            COMMIT,
            TPC_COMMIT])

    def test_remove_phonetic_one(self):
        add_fetch_result([(123, 0)])
        add_fetch_result([(1,)])
        add_fetch_result([None])

        dm, dmalt = _dm('value')

        self.assertEqual(
                datahog.name.remove(self.p, 123, 2, 'value'),
                True)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select 1
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dm, 'value', 123)),
            ROWCOUNT,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
with removal as (
    update name
    set time_removed=now()
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and value=%s
    returning pos
), bump as (
    update name
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
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update phonetic_lookup
set time_removed=now()
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dm, 'value', 123)),
            ROWCOUNT,
            COMMIT,
            TPC_COMMIT])

    def test_remove_phonetic_two(self):
        add_fetch_result([(123, 0)])
        add_fetch_result([(123, 0)])
        add_fetch_result([(1,)])
        add_fetch_result([None])
        add_fetch_result([None])

        dm, dma = _dm('window')

        self.assertEqual(
                datahog.name.remove(self.p, 123, 2, 'window'),
                True)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select 1
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dm, 'window', 123)),
            ROWCOUNT,
            COMMIT,
            GET_CURSOR,
            EXECUTE("""
select 1
from phonetic_lookup
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dma, 'window', 123)),
            ROWCOUNT,
            COMMIT,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
with removal as (
    update name
    set time_removed=now()
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and value=%s
    returning pos
), bump as (
    update name
    set pos = pos - 1
    where
        exists (select 1 from removal)
        and time_removed is null
        and base_id=%s
        and ctx=%s
        and pos > (select pos from removal)
)
select 1 from removal
""", (123, 2, 'window', 123, 2)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            TPC_BEGIN,
            GET_CURSOR,
            EXECUTE("""
update phonetic_lookup
set time_removed=now()
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dm, 'window', 123)),
            ROWCOUNT,
            TPC_PREPARE,
            RESET,
            GET_CURSOR,
            EXECUTE("""
update phonetic_lookup
set time_removed=now()
where
    time_removed is null
    and ctx=%s
    and code=%s
    and value=%s
    and base_id=%s
""", (2, dma, 'window', 123)),
            ROWCOUNT,
            COMMIT,
            TPC_COMMIT,
            TPC_COMMIT])


if __name__ == '__main__':
    unittest.main()

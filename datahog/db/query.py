# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

from ..const import context, storage, table, util


_missing = object() # default argument sentinel


def insert_entity(cursor, ctx, flags=0):
    cursor.execute("""
insert into entity (ctx, flags)
values (%s, %s)
returning guid
""", (ctx, flags))

    return cursor.fetchone()[0]


def select_entity(cursor, guid, ctx):
    cursor.execute("""
select flags
from entity
where
    time_removed is null
    and guid=%s
    and ctx=%s
""", (guid, ctx))

    if not cursor.rowcount:
        return None

    return {'guid': guid, 'ctx': ctx, 'flags': cursor.fetchone()[0]}


def remove_entity(cursor, guid, ctx):
    cursor.execute("""
update entity
set time_removed=now()
where
    time_removed is null
    and guid=%s
    and ctx=%s
""", (guid, ctx))

    return bool(cursor.rowcount)


def select_property(cursor, base_id, ctx):
    if util.ctx_storage(ctx) == storage.INT:
        val_field = 'num'
    else:
        val_field = 'value'

    cursor.execute("""
select %s, flags
from property
where
    time_removed is null
    and base_id=%%s
    and ctx=%%s
""" % (val_field,), (base_id, ctx))

    if not cursor.rowcount:
        return False, None, None

    value, flags = cursor.fetchone()
    return True, value, flags


def select_properties(cursor, base_id, ctxs):
    cursor.execute("""
select ctx, num, value, flags
from property
where
    time_removed is null
    and base_id=%%s
    and ctx in (%s)
""" % (','.join('%s' for c in sorted(ctxs)),), (base_id,) + tuple(ctxs))

    results = {ctx: {
            'base_id': base_id,
            'ctx': ctx,
            'flags': flags,
            'value': (num if util.ctx_storage(ctx) == storage.INT
                    else value),
        } for ctx, num, value, flags in cursor.fetchall()}

    return map(results.get, ctxs)


def upsert_property(cursor, base_id, ctx, value, flags):
    if util.ctx_storage(ctx) == storage.INT:
        val_field = 'num'
        other_field = 'value'
    else:
        val_field = 'value'
        other_field = 'num'
    base_tbl, base_ctx = util.ctx_base(ctx)
    base_tbl = table.NAMES[base_tbl]

    cursor.execute("""
with existencequery as (
    select 1
    from %s
    where
        time_removed is null
        and guid=%%s
        and ctx=%%s
),
updatequery as (
    update property
    set %s=%%s, %s=null, flags=%%s
    where
        time_removed is null
        and base_id=%%s
        and ctx=%%s
        and exists (select 1 from existencequery)
    returning 1
),
insertquery as (
    insert into property (base_id, ctx, %s, flags)
    select %%s, %%s, %%s, %%s
    where
        not exists (select 1 from updatequery)
        and exists (select 1 from existencequery)
    returning 1
)
select
    exists (select 1 from insertquery),
    exists (select 1 from updatequery)
""" % (base_tbl, val_field, other_field, val_field),
            (base_id, base_ctx, value, flags, base_id, ctx, base_id, ctx,
                value, flags))

    return cursor.fetchone()


def update_property(cursor, base_id, ctx, value):
    if util.ctx_storage(ctx) == storage.INT:
        val_field = 'num'
        other_field = 'value'
    else:
        val_field = 'value'
        other_field = 'num'

    cursor.execute("""
update property
set %s=%%s, %s=%%s
where
    time_removed is null
    and base_id=%%s
    and ctx=%%s
""" % (val_field, other_field), (value, None, base_id, ctx))

    return cursor.rowcount


def increment_property(cursor, base_id, ctx, by=1, limit=_missing):
    if limit is _missing:
        cursor.execute("""
update property
set num=num+%s
where
    time_removed is null
    and base_id=%s
    and ctx=%s
returning num
""", (by, base_id, ctx))

    else:
        op = '>' if by < 0 else '<'
        cursor.execute("""
update property
set num=case
    when (num+%%s %s %%s)
    then num+%%s
    else %%s
    end
where
    time_removed is null
    and base_id=%%s
    and ctx=%%s
returning num
""" % (op,), (by, limit, by, limit, base_id, ctx))

    if not cursor.rowcount:
        return None

    return cursor.fetchone()[0]


def remove_property(cursor, base_id, ctx, value=_missing):
    if value is _missing:
        where_value, params = "", (base_id, ctx)
    else:
        if util.ctx_storage(ctx) == storage.INT:
            where_value = "and num=%s"
            params = (base_id, ctx, value)
        elif value is None:
            where_value = "and value is null"
            params = (base_id, ctx)
        else:
            where_value = "and value=%s"
            params = (base_id, ctx, value)

    cursor.execute("""
update property
set time_removed=now()
where
    time_removed is null
    and base_id=%%s
    and ctx=%%s
    %s
""" % (where_value,), params)

    return bool(cursor.rowcount)


def remove_properties_multiple_bases(cursor, base_ids):
    cursor.execute("""
update property
set time_removed=now()
where
    time_removed is null
    and base_id in (%s)
""" % (','.join('%s' for x in base_ids),), base_ids)

    return cursor.rowcount


def select_alias_lookup(cursor, digest, ctx):
    cursor.execute("""
select base_id, flags
from alias_lookup
where
    time_removed is null
    and hash=%s
    and ctx=%s
""", (digest, ctx))

    if not cursor.rowcount:
        return None

    base_id, flags = cursor.fetchone()
    # caller has to add 'value' key, they only passed us the digest
    return {
        'base_id': base_id,
        'flags': flags,
        'ctx': ctx
    }


def select_aliases(cursor, base_id, ctx, limit, start):
    cursor.execute("""
select flags, value, pos
from alias
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and pos >= %s
order by pos asc
limit %s
""", (base_id, ctx, start, limit))

    return [{
            'base_id': base_id,
            'flags': flags,
            'ctx': ctx,
            'pos': pos,
            'value': value
        } for flags, value, pos in cursor.fetchall()]


def maybe_insert_alias_lookup(cursor, digest, ctx, base_id, flags):
    cursor.execute("""
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
""", (digest, ctx, digest, ctx, base_id, flags))

    if cursor.rowcount:
        return False, cursor.fetchone()[0]

    return True, base_id


def insert_alias(cursor, base_id, ctx, value, index, flags):
    base_tbl, base_ctx = util.ctx_base(ctx)
    base_tbl = table.NAMES[base_tbl]

    if index is None:
        cursor.execute("""
insert into alias (base_id, ctx, value, pos, flags)
select %%s, %%s, %%s, (
    select count(*)
    from alias
    where
        time_removed is null
        and base_id=%%s
        and ctx=%%s
), %%s
where exists (
    select 1 from %s
    where
        time_removed is null
        and guid=%%s
        and ctx=%%s
)
""" % (base_tbl,),
            (base_id, ctx, value, base_id, ctx, flags, base_id, base_ctx))
    else:
        cursor.execute("""
with existence as (
    select 1 from %s
    where
        time_removed is null
        and guid=%%s
        and ctx=%%s
), increment as (
update alias
set pos = pos + 1
where
    exists (select 1 from existence)
    and time_removed is null
    and base_id=%%s
    and ctx=%%s
    and pos >= %%s
)
insert into alias (base_id, ctx, value, pos, flags)
select %%s, %%s, %%s, %%s, %%s
where exists (select 1 from existence)
returning 1
""" % (base_tbl,), (
            base_id, base_ctx,
            base_id, ctx, index,
            base_id, ctx, value, index, flags))

    return bool(cursor.rowcount)


def reorder_alias(cursor, base_id, ctx, value, pos):
    cursor.execute("""
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
), move as (
    update alias
    set pos=%s
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and value=%s
    returning 1
)
select exists (select 1 from move)
""", (base_id, ctx, value, base_id, ctx, pos, pos, base_id, ctx, value))

    return cursor.fetchone()[0]


def remove_alias_lookup(cursor, digest, ctx, base_id):
    cursor.execute("""
update alias_lookup
set time_removed=now()
where
    time_removed is null
    and hash=%s
    and ctx=%s
    and base_id=%s
""", (digest, ctx, base_id))

    return bool(cursor.rowcount)


def remove_alias(cursor, base_id, ctx, value):
    cursor.execute("""
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
""", (base_id, ctx, value, base_id, ctx))

    return bool(cursor.rowcount)


def remove_alias_lookups_multi(cursor, aliases):
    flat_als = reduce(lambda a, b: a.extend(b) or a, aliases, [])

    cursor.execute("""
update alias_lookup
set time_removed=now()
where
    time_removed is null
    and (hash, ctx) in (%s)
returning hash, ctx
""" % (','.join('(%s, %s)' for x in aliases),), flat_als)

    return cursor.fetchall()


def remove_aliases_multiple_bases(cursor, base_ids):
    cursor.execute("""
update alias
set time_removed=now()
where
    time_removed is null
    and base_id in (%s)
returning value, ctx
""" % (','.join('%s' for x in base_ids),), base_ids)

    return cursor.fetchall()


def insert_relationship(cursor, base_id, rel_id, ctx, forward, index, flags):
    if forward:
        guid_tbl, guid_ctx = util.ctx_base(ctx)
        guid = base_id
        guid_col = 'base_id'
    else:
        guid_tbl, guid_ctx = util.ctx_rel(ctx)
        guid = rel_id
        guid_col = 'rel_id'
    guid_tbl = table.NAMES[guid_tbl]

    if index is None:
        cursor.execute("""
insert into relationship (base_id, rel_id, ctx, forward, pos, flags)
select %%s, %%s, %%s, %%s, (
    select count(*)
    from relationship
    where
        time_removed is null
        and %s=%%s
        and ctx=%%s
        and forward=%%s
), %%s
where exists (
    select 1
    from %s
    where
        time_removed is null
        and guid=%%s
        and ctx=%%s
)
returning 1
""" % (guid_col, guid_tbl), (
        base_id, rel_id, ctx, forward,
        guid, ctx, forward,
        flags,
        guid, guid_ctx))

    else:
        cursor.execute("""
with eligible as (
    select 1
    from %s
    where
        time_removed is null
        and guid=%%s
        and ctx=%%s
), bump as (
    update relationship
    set pos=pos + 1
    where
        exists (select 1 from eligible)
        and time_removed is null
        and forward=%%s
        and %s=%%s
        and ctx=%%s
        and pos >= %%s
)
insert into relationship (base_id, rel_id, ctx, forward, pos, flags)
select %%s, %%s, %%s, %%s, %%s, %%s
where exists (select 1 from eligible)
returning 1
""" % (guid_tbl, guid_col), (guid, guid_ctx,
            forward, guid, ctx, index,
            base_id, rel_id, ctx, forward, index, flags))

    return cursor.rowcount


def select_relationships(cursor, guid, ctx, forward, limit, start, other_guid=_missing):
    here_name = "base_id" if forward else "rel_id"
    other_name = "rel_id" if forward else "base_id"

    if other_guid is _missing:
        clause = ""
        params = (guid, ctx, forward, start, limit)
    else:
        clause = "and %s=%%s" % (other_name,)
        params = (guid, ctx, forward, start, other_guid, limit)

    cursor.execute("""
select %s, flags, pos
from relationship
where
    time_removed is null
    and %s=%%s
    and ctx=%%s
    and forward=%%s
    and pos >= %%s
    %s
order by pos asc
limit %%s
""" % (other_name, here_name, clause), params)

    return [{
            here_name: guid,
            'flags': flags,
            other_name: other_guid,
            'ctx': ctx,
            'pos': pos}
        for other_guid, flags, pos in cursor.fetchall()]


def remove_relationship(cursor, base_id, rel_id, ctx, forward):
    if forward:
        anchor_guid = base_id
        anchor_col = "base_id"
    else:
        anchor_guid = rel_id
        anchor_col = "rel_id"

    cursor.execute("""
with removal as (
    update relationship
    set time_removed=now()
    where
        time_removed is null
        and base_id=%%s
        and ctx=%%s
        and forward=%%s
        and rel_id=%%s
    returning pos
), bump as (
    update relationship
    set pos = pos - 1
    where
        exists (select 1 from removal)
        and time_removed is null
        and %s=%%s
        and ctx=%%s
        and forward=%%s
        and pos > (select pos from removal)
)
select 1 from removal
""" % (anchor_col,), (
        base_id, ctx, forward, rel_id,
        anchor_guid, ctx, forward))

    return bool(cursor.rowcount)


def remove_relationships_multiple_bases(cursor, base_ids):
    cursor.execute("""
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
""" % ((','.join('%s' for x in base_ids),) * 2), base_ids * 2)

    return cursor.fetchall()


def remove_relationships_multi(cursor, rels):
    flat_rels = reduce(lambda a, b: a.extend(b) or a, rels, [])

    cursor.execute("""
update relationship
set time_removed=now()
where
    time_removed is null
    and (base_id, ctx, forward, rel_id) in (%s)
""" % (','.join('(%s, %s, %s, %s)' for x in rels),), flat_rels)

    return cursor.rowcount


def bulk_reorder_relationships(cursor, pairs, forward):
    anchor_col = "base_id" if forward else "rel_id"
    data_col = "rel_id" if forward else "base_id"

    replace = ','.join('(%s,%s)' for p in pairs)
    flat_pairs = reduce(lambda a, b: a.extend(b) or a, pairs, [])

    cursor.execute("""
update relationship
set pos = ordering.counter - 1
from (
    select row_number() over (
        partition by (%s, ctx)
        order by pos asc
    ) counter, %s
    from relationship
    where
        time_removed is null
        and forward=%%s
        and (%s, ctx) in (%s)
) as ordering
where
    relationship.%s = ordering.%s
    and relationship.time_removed is null
    and relationship.forward=%%s
    and (relationship.%s, relationship.ctx) in (%s)
returning 1
""" % (anchor_col, data_col, anchor_col, replace, data_col, data_col,
            anchor_col, replace),
        ([forward] + flat_pairs) * 2)

    return cursor.rowcount


def reorder_relationship(cursor, base_id, rel_id, ctx, forward, pos):
    anchor_col = "base_id" if forward else "rel_id"
    anchor_guid = base_id if forward else rel_id

    cursor.execute("""
with oldpos as (
    select pos
    from relationship
    where
        time_removed is null
        and forward=%%s
        and base_id=%%s
        and ctx=%%s
        and rel_id=%%s
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
        and forward=%%s
        and %s=%%s
        and ctx=%%s
        and pos between symmetric (select pos from oldpos) and %%s
    returning 1
), move as (
    update relationship
    set pos=%%s
    where
        time_removed is null
        and forward=%%s
        and base_id=%%s
        and ctx=%%s
        and rel_id=%%s
    returning 1
)
select exists (select 1 from move)
""" % (anchor_col,), (
        forward, base_id, ctx, rel_id,
        forward, anchor_guid, ctx, pos,
        pos,
        forward, base_id, ctx, rel_id))

    return cursor.fetchone()[0]


def insert_node(cursor, base_id, ctx, value, flags):
    if util.ctx_storage(ctx) == storage.INT:
        val_field = 'num'
    else:
        val_field = 'value'
    base_tbl, base_ctx = util.ctx_base(ctx)
    base_tbl = table.NAMES[base_tbl]

    cursor.execute("""
insert into node (ctx, %s, flags)
select %%s, %%s, %%s
where exists (
    select 1
    from %s
    where
        time_removed is null
        and guid=%%s
        and ctx=%%s
)
returning guid
""" % (val_field, base_tbl), (ctx, value, flags, base_id, base_ctx))

    if not cursor.rowcount:
        return None

    return {
        'guid': cursor.fetchone()[0],
        'ctx': ctx,
        'flags': flags,
        'value': value,
    }


def insert_edge(cursor, base_id, ctx, child_id, pos=None, base_ctx=None):
    if base_ctx is None:
        where, where_params = 'true', ()
    else:
        where = '''exists(
    select 1 from %s
    where
        time_removed is null
        and guid=%%s
        and ctx=%%s
)''' % table.NAMES[util.ctx_tbl(base_ctx)]
        where_params = (base_id, ctx)

    if pos is None:
        cursor.execute('''
insert into edge (base_id, ctx, child_id, pos)
select %%s, %%s, %%s, (
    select count(*)
    from edge
    where
        time_removed is null
        and base_id=%%s
        and ctx=%%s
)
where %s
''' % (where,), (base_id, ctx, child_id, base_id, ctx) + where_params)
    else:
        cursor.execute('''
with bump as (
    update edge
    set pos=pos + 1
    where
        time_removed is null
        and base_id=%%s
        and ctx=%%s
        and pos >= %%s
        and %s
)
insert into edge (base_id, ctx, child_id, pos)
select %%s, %%s, %%s, %%s
where %s
returning 1
''' % (where, where), (base_id, ctx, pos) + where_params + (
            base_id, ctx, child_id, pos) + where_params)

    return bool(cursor.rowcount)


def select_node(cursor, nid, ctx):
    if util.ctx_storage(ctx) == storage.INT:
        val_field = 'num'
    else:
        val_field = 'value'

    cursor.execute("""
select flags, %s
from node
where
    time_removed is null
    and guid=%%s
    and ctx=%%s
""" % (val_field,), (nid, ctx))

    if not cursor.rowcount:
        return None

    flags, value = cursor.fetchone()

    return {
        'guid': nid,
        'ctx': ctx,
        'flags': flags,
        'value': value
    }


def select_nodes(cursor, guid_ctx_pairs):
    flat_pairs = reduce(lambda a, b: a.extend(b) or a, guid_ctx_pairs, [])

    #TODO: EXPLAIN this query
    cursor.execute("""
select guid, ctx, flags, num, value
from node
where
    time_removed is null
    and (guid, ctx) in (%s)
""" % (','.join('(%s, %s)' for p in guid_ctx_pairs),), flat_pairs)

    return [{
        'guid': guid,
        'ctx': ctx,
        'flags': flags,
        'value': num if util.ctx_storage(ctx) == storage.INT else val,
        } for guid, ctx, flags, num, val in cursor.fetchall()]


def select_node_guids(cursor, base_id, limit, pos, ctx):
    cursor.execute("""
select child_id, ctx, pos
from edge
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and pos >= %s
order by pos asc
limit %s
""", (base_id, ctx, pos, limit))

    return cursor.fetchall()


def update_node(cursor, nid, ctx, value, old_value=_missing):
    int_storage = util.ctx_storage(ctx) == storage.INT
    if int_storage:
        val_field = 'num'
        other_field = 'value'
    else:
        val_field = 'value'
        other_field = 'num'

    if old_value is _missing:
        oldval_where = ""
        params = (value, nid, ctx)
    else:
        oldval_where = 'and %s=%%s' % (val_field,)
        params = (value, nid, ctx, old_value)

    cursor.execute("""
update node
set %s=%%s, %s=null
where
    time_removed is null
    and guid=%%s
    and ctx=%%s
    %s
""" % (val_field, other_field, oldval_where), params)

    return bool(cursor.rowcount)


def increment_node(cursor, nid, ctx, by=1, limit=_missing):
    if limit is _missing:
        cursor.execute("""
update node
set num=num+%s
where
    time_removed is null
    and guid=%s
    and ctx=%s
returning num
""", (by, nid, ctx))

    else:
        op = '>' if by < 0 else '<'
        cursor.execute("""
update node
set num=case
    when (num+%%s %s %%s)
    then num+%%s
    else %%s
    end
where
    time_removed is null
    and guid=%%s
    and ctx=%%s
returning num
""" % (op,), (by, limit, by, limit, nid, ctx))

    if not cursor.rowcount:
        return None

    return cursor.fetchone()[0]


def reorder_edge(cursor, base_id, ctx, child_id, pos):
    cursor.execute("""
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
""", (base_id, ctx, child_id,
    base_id, ctx, pos,
    pos, base_id, ctx, child_id))

    return cursor.fetchone()[0]


def remove_edge(cursor, base_id, ctx, child_id):
    cursor.execute("""
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
""", (base_id, ctx, child_id, base_id, ctx))

    return bool(cursor.rowcount)


def remove_edges_multiple_bases(cursor, base_ids):
    cursor.execute("""
update edge
set time_removed=now()
where
    time_removed is null
    and base_id in (%s)
returning child_id
""" % (','.join('%s' for b in base_ids),), base_ids)

    return [r[0] for r in cursor.fetchall()]


def remove_node(cursor, nid, ctx):
    cursor.execute("""
update node
set time_removed=now()
where
    time_removed is null
    and guid=%%s
    and ctx=%%s
""", (nid, ctx))

    return bool(cursor.rowcount)


def remove_nodes(cursor, nodes):
    cursor.execute("""
update node
set time_removed=now()
where
    time_removed is null
    and guid in (%s)
returning guid
""" % (','.join('%s' for n in nodes),), nodes)

    return [r[0] for r in cursor.fetchall()]


def insert_name(cursor, base_id, ctx, value, flags, index):
    base_tbl, base_ctx = util.ctx_base(ctx)
    base_tbl = table.NAMES[base_tbl]

    if index is None:
        cursor.execute("""
insert into name (base_id, ctx, value, flags, pos)
select %%s, %%s, %%s, %%s, (
    select count(*)
    from name
    where
        time_removed is null
        and base_id=%%s
        and ctx=%%s
)
where exists (
    select 1 from %s
    where
        time_removed is null
        and guid=%%s
        and ctx=%%s
)
""" % (base_tbl,), (
            base_id, ctx, value, flags,
            base_id, ctx,
            base_id, base_ctx))
    else:
        cursor.execute("""
with existence as (
    select 1 from %s
    where
        time_removed is null
        and guid=%%s
        and ctx=%%s
), increment as (
update name
set pos = pos + 1
where
    exists (select 1 from existence)
    and time_removed is null
    and base_id=%%s
    and ctx=%%s
    and pos >= %%s
)
insert into name (base_id, ctx, value, flags, pos)
select %%s, %%s, %%s, %%s, %%s
where exists (select 1 from existence)
returning 1
""" % (base_tbl,), (
            base_id, base_ctx,
            base_id, ctx, index,
            base_id, ctx, value, flags, index))

    return cursor.rowcount


def insert_prefix_lookup(cursor, value, flags, ctx, base_id):
    cursor.execute("""
insert into prefix_lookup (value, flags, ctx, base_id)
values (%s, %s, %s, %s)
""", (value, flags, ctx, base_id))


def search_prefixes(cursor, value, ctx, limit, start):
    cursor.execute("""
select base_id, flags, value
from prefix_lookup
where
    time_removed is null
    and ctx=%s
    and value like %s || '%%'
    and value > %s
order by value
limit %s
""", (ctx, value, start, limit))

    return [{
            'base_id': base_id,
            'flags': flags,
            'value': value,
            'ctx': ctx,
        } for base_id, flags, value in cursor.fetchall()]


def add_flags(cursor, table, flags, where):
    clause, values = ['time_removed is null'], [flags]
    for key, value in where.items():
        if value is None:
            clause.append("%s is null" % key)
        else:
            clause.append("%s=%%s" % key)
            values.append(value)
    clause = ' and '.join(clause)

    cursor.execute("""
update %s
set flags=flags | %%s
where %s
returning flags
""" % (table, clause), values)

    return [x[0] for x in cursor.fetchall()]


def clear_flags(cursor, table, flags, where):
    clause, values = ['time_removed is null'], [flags]
    for key, value in where.items():
        if value is None:
            clause.append("%s is null" % key)
        else:
            clause.append("%s=%%s" % key)
            values.append(value)
    clause = ' and '.join(clause)

    cursor.execute("""
update %s
set flags=flags & ~%%s
where %s
returning flags
""" % (table, clause), values)

    return [x[0] for x in cursor.fetchall()]

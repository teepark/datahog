# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

from .const import context, table, util


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
    if util.ctx_storage(ctx) == context.STORAGE_INT:
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


def upsert_property(cursor, base_id, ctx, value, flags):
    if util.ctx_storage(ctx) == context.STORAGE_INT:
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
    if util.ctx_storage(ctx) == context.STORAGE_INT:
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
        if util.ctx_storage(ctx) == context.STORAGE_INT:
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


def select_aliases(cursor, base_id, ctx):
    cursor.execute("""
select flags, value
from alias
where
    time_removed is null
    and base_id=%s
    and ctx=%s
""", (base_id, ctx))

    return [{
            'base_id': base_id,
            'flags': flags,
            'ctx': ctx,
            'value': value
        } for flags, value in cursor.fetchall()]


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


def insert_alias(cursor, base_id, ctx, value, flags):
    base_tbl, base_ctx = util.ctx_base(ctx)
    base_tbl = table.NAMES[base_tbl]

    cursor.execute("""
insert into alias (base_id, ctx, value, flags)
select %%s, %%s, %%s, %%s
where exists (
    select 1 from %s
    where
        time_removed is null
        and guid=%%s
        and ctx=%%s
)
""" % (base_tbl,), (base_id, ctx, value, flags, base_id, base_ctx))

    return cursor.rowcount


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
update alias
set time_removed=now()
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and value=%s
""", (base_id, ctx, value))

    return cursor.rowcount


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


def insert_relationship(cursor, base_id, rel_id, ctx, forward, flags):
    if forward:
        guid_tbl, guid_ctx = util.ctx_base(ctx)
        guid = base_id
    else:
        guid_tbl, guid_ctx = util.ctx_rel(ctx)
        guid = rel_id
    guid_tbl = table.NAMES[guid_tbl]

    cursor.execute("""
insert into relationship (base_id, rel_id, ctx, forward, flags)
select %%s, %%s, %%s, %%s, %%s
where
    exists (
        select 1
        from %s
        where
            time_removed is null
            and guid=%%s
            and ctx=%%s
    )
returning 1
""" % (guid_tbl,), (
        base_id, rel_id, ctx, forward, flags,
        guid, guid_ctx))

    return cursor.rowcount


def select_relationships(cursor, guid, ctx, forward, other_guid=_missing):
    here_name = "base_id" if forward else "rel_id"
    other_name = "rel_id" if forward else "base_id"

    if other_guid is _missing:
        clause = ""
        params = (guid, ctx, forward)
    else:
        clause = "and %s=%%s" % (other_name,)
        params = (guid, ctx, forward, other_guid)

    cursor.execute("""
select %s, flags
from relationship
where
    time_removed is null
    and %s=%%s
    and ctx=%%s
    and forward=%%s
    %s
""" % (other_name, here_name, clause), params)

    return [{
            here_name: guid,
            'flags': flags,
            other_name: other_guid,
            'ctx': ctx}
        for other_guid, flags in cursor.fetchall()]


def remove_relationship(cursor, base_id, rel_id, ctx, forward):
    cursor.execute("""
update relationship
set time_removed=now()
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and forward=%s
    and rel_id=%s
""", (base_id, ctx, forward, rel_id))

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


def insert_tree_node(cursor, base_id, ctx, value, flags):
    if util.ctx_storage(ctx) == context.STORAGE_INT:
        val_field = 'num'
    else:
        val_field = 'value'
    base_tbl, base_ctx = util.ctx_base(ctx)
    base_tbl = table.NAMES[base_tbl]

    cursor.execute("""
insert into tree_node (ctx, %s, flags)
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


def insert_tree_edge(cursor, base_id, ctx, child_id, base_ctx=None):
    if base_ctx is None:
        where = ''
        params = (base_id, ctx, child_id)
    else:
        base_tbl = table.NAMES[util.ctx_tbl(base_ctx)]
        where = '''where exists (
    select 1 from %s
    where
        time_removed is null
        and guid=%%s
        and ctx=%%s
)''' % (base_tbl,)
        params = (base_id, ctx, child_id, base_id, base_ctx)

    cursor.execute("""
insert into tree_edge (base_id, ctx, child_id)
select %%s, %%s, %%s
%s
""" % (where,), params)

    return bool(cursor.rowcount)


def select_tree_node(cursor, nid, ctx):
    if util.ctx_storage(ctx) == context.STORAGE_INT:
        val_field = 'num'
    else:
        val_field = 'value'

    cursor.execute("""
select flags, %s
from tree_node
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


def select_tree_nodes(cursor, guid_ctx_pairs):
    flat_pairs = reduce(lambda a, b: a.extend(b) or a, guid_ctx_pairs, [])

    #TODO: EXPLAIN this query
    cursor.execute("""
select guid, ctx, flags, num, value
from tree_node
where
    time_removed is null
    and (guid, ctx) in (%s)
""" % (','.join('(%s, %s)' for p in guid_ctx_pairs),), flat_pairs)

    return [{
        'guid': guid,
        'ctx': ctx,
        'flags': flags,
        'value': num if util.ctx_storage(ctx) == context.STORAGE_INT else val,
        } for guid, ctx, flags, num, val in cursor.fetchall()]


def select_tree_node_guids(cursor, base_id, ctx=_missing):
    if ctx is _missing:
        where_ctx = ""
        params = (base_id,)
    else:
        where_ctx = "and ctx=%s"
        params = (base_id, ctx)

    cursor.execute("""
select child_id, ctx
from tree_edge
where
    time_removed is null
    and base_id=%%s
    %s
""" % (where_ctx,), params)

    return cursor.fetchall()


def update_tree_node(cursor, nid, ctx, value, old_value=_missing):
    int_storage = util.ctx_storage(ctx) == context.STORAGE_INT
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
update tree_node
set %s=%%s, %s=null
where
    time_removed is null
    and guid=%%s
    and ctx=%%s
    %s
""" % (val_field, other_field, oldval_where), params)

    return bool(cursor.rowcount)


def increment_tree_node(cursor, nid, ctx, by=1, limit=_missing):
    if limit is _missing:
        cursor.execute("""
update tree_node
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
update tree_node
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


def remove_tree_edge(cursor, base_id, ctx, child_id):
    cursor.execute("""
update tree_edge
set time_removed=now()
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and child_id=%s
""", (base_id, ctx, child_id))

    return bool(cursor.rowcount)


def remove_tree_edges_multiple_bases(cursor, base_ids):
    cursor.execute("""
update tree_edge
set time_removed=now()
where
    time_removed is null
    and base_id in (%s)
returning child_id
""" % (','.join('%s' for b in base_ids),), base_ids)

    return [r[0] for r in cursor.fetchall()]


def remove_tree_node(cursor, nid, ctx):
    cursor.execute("""
update tree_node
set time_removed=now()
where
    time_removed is null
    and guid=%%s
    and ctx=%%s
""", (nid, ctx))

    return bool(cursor.rowcount)


def remove_tree_nodes(cursor, nodes):
    cursor.execute("""
update tree_node
set time_removed=now()
where
    time_removed is null
    and guid in (%s)
returning guid
""" % (','.join('%s' for n in nodes),), nodes)

    return [r[0] for r in cursor.fetchall()]


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


create extension if not exists fuzzystrmatch;

create sequence guids maxvalue %(max)d start with %(start)d;


-- ENTITIES --

create table entity (
  guid bigint not null default nextval('guids'),
  flags smallint default 0 not null,
  time_removed timestamp default null,
  ctx smallint not null
);

create unique index entity_guid on entity (
  guid
) where time_removed is null;


-- PROPERTIES --

create table property (
  base_id bigint not null,
  flags smallint default 0 not null,
  time_removed timestamp default null,
  ctx smallint not null,
  num bigint default null,
  value bytea default null,
  check (num is null or value is null)
);

create unique index property_uniq on property (
  base_id, ctx
) where time_removed is null;


-- ALIASES --

create table alias (
  base_id bigint not null,
  flags smallint default 0 not null,
  time_removed timestamp default null,
  ctx smallint not null,
  pos int not null,
  value varchar(255) not null
);

create index alias_idx on alias (
  base_id, ctx, pos
) where time_removed is null;

create table alias_lookup (
  hash bytea not null,
  flags smallint default 0 not null,
  time_removed timestamp default null,
  ctx smallint not null,
  base_id bigint not null
);

create unique index alias_lookup_uniq on alias_lookup (
  hash, ctx
) where time_removed is null;


-- RELATIONSHIPS --

create table relationship (
  base_id bigint not null,
  flags smallint default 0 not null,
  time_removed timestamp default null,
  rel_id bigint not null,
  ctx smallint not null,
  pos int not null,
  forward bool not null
);

create unique index relationship_uniq_forward on relationship (
  base_id, ctx, rel_id
) where time_removed is null and forward=true;

create index relationship_forward_idx on relationship (
  base_id, ctx, pos
) where time_removed is null and forward=true;

create unique index relationship_uniq_backward on relationship (
  rel_id, ctx, base_id
) where time_removed is null and forward=false;

create index relationship_backward_idx on relationship (
  rel_id, ctx, pos
) where time_removed is null and forward=false;


-- NODES --

create table node (
  guid bigint not null default nextval('guids'),
  flags smallint default 0 not null,
  time_removed timestamp default null,
  ctx smallint not null,
  num bigint default null,
  value bytea default null,
  check (num is null or value is null)
);

create unique index node_guid on node (
  guid
) where time_removed is null;

create table edge (
  base_id bigint not null,
  time_removed timestamp default null,
  ctx smallint not null,
  child_id bigint not null,
  pos int not null
);

create index edge_idx on edge (
  base_id, ctx, pos
) where time_removed is null;

create index edge_child on edge (
  child_id
) where time_removed is null;


-- NAMES --

create table name (
  base_id bigint not null,
  flags smallint default 0 not null,
  time_removed timestamp default null,
  ctx smallint not null,
  pos int not null,
  value varchar(255) not null
);

create index name_idx on name (
  base_id, ctx, pos
) where time_removed is null;

create unique index name_uniq on name (
  base_id, ctx, value
) where time_removed is null;
-- TODO: is this index a great idea? I don't see that this gives us any
--       better lookups than name_idx, it's only here to enforce uniqueness

create table prefix_lookup (
  value varchar(255) not null,
  flags smallint default 0 not null,
  time_removed timestamp default null,
  ctx smallint not null,
  base_id bigint not null
);

create index prefix_lookup_idx on prefix_lookup (
  ctx, value
) where time_removed is null;

create table phonetic_lookup(
  code varchar(4) not null,
  value varchar(255) not null,
  flags smallint default 0 not null,
  time_removed timestamp default null,
  ctx smallint not null,
  base_id bigint not null
);

create index phonetic_lookup_idx on phonetic_lookup(
  ctx, code, base_id
) where time_removed is null;

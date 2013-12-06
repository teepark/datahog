# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

import mummy

from . import search, storage, table


META = {}


def set_context(title, value, tbl, meta=None):
    '''create a constant for use in 'ctx'

    :param str title:
        the constant name. will be made available as
        datahog.const.context.NAME.

    :param int value: the integer value to place in the 'ctx' column

    :param int tbl:
        the table for which this context applies (must be a table from
        datahog.const.table)

    :param dict meta:
        dict for specifying other meta-data about the context.

        possible values:

            base_ctx
                the context value of the object to which it is related through
                its ``base_id``. applies when ``tbl`` is ``table.NODE``,
                ``table.PROPERTY``, ``table.ALIAS``, or
                ``table.RELATIONSHIP``.

            rel_ctx
                the context value of the object to which it is related through
                its ``rel_id``. applies when ``tbl`` is
                ``table.RELATIONSHIP``.

            storage
                defines behavior of the int/str storage columns. must be one of
                ``NULL``, ``INT``, ``STR``, ``UTF``, ``SERIAL``. applies when
                ``tbl`` is ``table.PROPERTY`` or ``table.NODE``.

            schema
                in the event of ``'storage': SERIAL``, a schema can be provided,
                against which values will be validated, and which
                will also be used to further compress values in the db.

            search
                defines the behavior of name.search(). must be one of the
                search constants ``PREFIX`` or ``PHONETIC``. only applies when
                ``tbl`` is ``table.NAME``.

                using ``search.PHONETIC`` requires that the ``fuzzy`` python
                library be installed.

            phonetic_loose
                for ``table.NAME`` and ``search.PHONETIC``, setting this to
                ``True`` (default ``False``) enables looser phonetic matching.
    '''
    if value in META:
        raise ValueError("duplicate context values: %s, %s" %
                (META[value][0], title))

    if tbl not in table.REVERSE:
        raise ValueError("unrecognized table const: %r" % tbl)

    if meta:
        for rel in ('base', 'rel'):
            ctxkey = '%s_ctx' % (rel,)
            if ctxkey not in meta:
                continue

            if meta[ctxkey] not in META:
                raise ValueError("related %s context %d doesn't exist" %
                        (rel, meta[ctxkey]))

        if meta.get('storage', storage.NULL) not in storage.ALL:
            raise ValueError("unrecognized storage type: %d" % meta['storage'])

        if 'schema' in meta:
            meta['schema'] = type(title.title() + 'Schema', (mummy.Message,),
                    {'SCHEMA': meta['schema']})

        if meta.get('search') == search.PHONETIC:
            # just so that this blows up nice and early
            import fuzzy

    META[value] = (title, tbl, meta)

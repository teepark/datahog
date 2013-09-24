# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4


REVERSE = {}
NAMES = {}


def _set_table(title, value, table_name):
    if title in globals():
        raise ValueError("context name already in use: %s" % title)

    if value in REVERSE:
        raise ValueError('duplicate table values: %s, %s' %
                (REVERSE[value], title))

    globals()[title] = value
    REVERSE[value] = title
    NAMES[value] = table_name


_set_table("ENTITY", 1, "entity")
_set_table("NODE", 2, "node")
_set_table("PROPERTY", 3, "property")
_set_table("ALIAS", 4, "alias")
_set_table("RELATIONSHIP", 5, "relationship")

# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

from .api import alias, entity, name, node, prop, relationship
from .const import *
from .pool import *

# set the table consts themselves here as well
for val, title in table.REVERSE.items():
    globals()[title] = val

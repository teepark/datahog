#!/usr/bin/env python
# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import os
from setuptools import setup


VERSION = (0, 0, 1, "")


setup(
    name="datahog",
    description="a shardable postgresql-backed data store",
    packages=["datahog", "datahog.const"],
    version='.'.join(filter(None, map(str, VERSION))),
)

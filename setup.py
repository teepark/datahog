#!/usr/bin/env python
# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import os
from setuptools import setup


VERSION = (0, 0, 2, "")


setup(
    name="datahog",
    description="a shardable postgresql-backed data store",
    packages=["datahog", "datahog.api", "datahog.const", "datahog.db"],
    version='.'.join(filter(None, map(str, VERSION))),
    install_requires=['psycopg2', 'mummy'],
    tests_require=install_requires+['greenhouse', 'fuzzy'],
)

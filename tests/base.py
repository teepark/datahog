# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import copy
import os
import sys
import unittest

import datahog

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pgmock import *
activate()


class TestCase(unittest.TestCase):
    CONFIG = {
        'shards': [{
            'shard': 0,
            'count': 2,
            'host': None,
            'port': None,
            'user': None,
            'password': None,
            'database': None,
        }],
        'lookup_insertion_plans': [[(0, 1)]],
        'shard_bits': 8,
        'digest_key': 'digest key',
    }
    maxDiff = None

    def setUp(self):
        self.p = datahog.GreenhouseConnPool(copy.deepcopy(self.CONFIG))
        self.p.start()
        self.p.wait_ready()
        reset()

    def tearDown(self):
        self.assertEqual(len(self.p._conns[0]._data), 2)
        self.p = None
        datahog.context.META.clear()
        datahog.flag.META.clear()
        reset()

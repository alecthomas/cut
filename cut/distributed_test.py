# encoding: utf-8
#
# Copyright (C) 2010 Alec Thomas <alec@swapoff.org>
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.
#
# Author: Alec Thomas <alec@swapoff.org>

__author__ = 'Alec Thomas <alec@swapoff.org>'


import unittest
from cut.distributed import Lock, dumps, loads


class TestDistributedSerialization(unittest.TestCase):
    EXPECTED_JSON = '{"__distributed_type__": "Lock", "state": {"key": "test"}}'
    EXPECTED_POD_JSON = '[1, "hello"]'

    def testdumps_distributed_primitive(self):
        lock = Lock('test')
        self.assertEqual(dumps(lock), self.EXPECTED_JSON)

    def testloads_distributed_primitive(self):
        expected_lock = Lock('test')
        lock = loads(self.EXPECTED_JSON)
        self.assertTrue(isinstance(lock, Lock))
        self.assertEqual(lock.key, expected_lock.key)

    def testdumps_pod(self):
        self.assertEqual(dumps([1, 'hello']), self.EXPECTED_POD_JSON)

    def testloads_pod(self):
        self.assertEqual(loads(self.EXPECTED_POD_JSON), [1, 'hello'])


if __name__ == '__main__':
    unittest.main()

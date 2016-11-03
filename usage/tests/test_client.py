import os
import random
import unittest


# This is to satisfy flask db
os.environ['APP_SETTINGS'] = 'config.py'
TEST_API_URL = 'http://144.6.236.232/bman/api'

from ..types import Client, BmanClient

class TestUsages(unittest.TestCase):
    def test_can_call(self):
        try:
            c = Client('https://slack.com/api/test')
            c.get()
        except:
            self.assertTrue(False)

    def test_can_group(self):
        import math
        c = Client('')
        count = 10
        size = 3
        g = c.group([i for i in range(count)], size=size)
        self.assertEqual(len(g), math.ceil(count/size))
        self.assertEqual(len(g[3]), count % size)

    def test_bman(self):
        c = BmanClient(TEST_API_URL)
        for role in random.sample(range(1000), 300):
            org_ids = c.get_parent_org_ids(role)
            print(org_ids)
            self.assertIsNotNone(org_ids)
            org_names = c.get_org_names(org_ids)
            self.assertIsNotNone(org_names)
            print(org_names)

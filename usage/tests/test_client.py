import unittest

from usage.types import Client

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

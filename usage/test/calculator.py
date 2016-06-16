import unittest

from usage.types import NovaUsage


class TestUsages(unittest.TestCase):
    def test_usage_calculate_return_list(self):
        nova = NovaUsage()
        usages = nova.calculate()
        self.assertIsInstance(usages, list)

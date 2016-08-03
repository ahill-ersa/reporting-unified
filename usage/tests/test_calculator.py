import unittest

from usage.types import Usage


class TestUsages(unittest.TestCase):
    def test_usage_calculate_return_list(self):
        # Test the base class which does nothing but defining interface
        nova = Usage(1, 2)
        usages = nova.calculate()
        self.assertIsInstance(usages, list)

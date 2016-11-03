import os
import unittest

os.environ['APP_SETTINGS'] = 'config.py'

from ..types import NovaUsage

class TestUsages(unittest.TestCase):
    def test_filename_of_usage_save(self):
        file_name = 'NovaUsage_1_2.json'
        nova = NovaUsage(1, 2, None)
        file_name = nova.save([])
        self.assertEqual(file_name, file_name)
        self.assertTrue(os.path.exists(file_name))
        os.remove(file_name)

import unittest

from ..apis import instance_method


class MethodsTestCase(unittest.TestCase):
    def test_instance_method_kwargs(self):
        rslt = instance_method(None, 'list', 'someid')
        self.assertEqual(rslt, [])

        rslt = instance_method(None, 'list', 'someid', default='a string')
        self.assertEqual(rslt, 'a string')

        rslt = instance_method(None, 'list', 'someid', my_key1='my_value2', my_key2='my_value2', default='reorder')
        self.assertEqual(rslt, 'reorder')

    def test_instance_method_bad_id(self):
        from ..models.xfs import Filesystem
        data = instance_method(Filesystem, 'list', 'someid')
        self.assertTrue(isinstance(data, list))
        self.assertEqual(len(data), 0)

        data = instance_method(Filesystem, 'list', 'aeb7cf1c-a842-4592-82e9-55d2dad00150')
        self.assertTrue(isinstance(data, list))
        self.assertEqual(len(data), 0)

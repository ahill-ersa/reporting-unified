import unittest

from ...models.hnas import Filesystem, FilesystemUsage, VirtualVolume, VirtualVolumeUsage
from ...tests import now, now_minus_24hrs


class FilesystemTestCase(unittest.TestCase):
    def test_now(self):
        fs = Filesystem.query.first()
        rslt = fs.summarise(now)
        self.assertEqual(rslt, {})

    def test_now_minus_24hrs(self):
        fss = Filesystem.query.limit(10)
        for fs in fss:
            rslt = fs.summarise(now_minus_24hrs, now)
            self.assertTrue(isinstance(rslt, dict))
            if rslt:
                self.assertIsNot(rslt, {})
                for v in rslt.values():
                    self.assertIsNotNone(v)


class VirtualVolumeTestCase(unittest.TestCase):
    def test_now(self):
        vv = VirtualVolume.query.first()
        rslt = vv.summarise(now)
        self.assertEqual(rslt, [])

    def test_now_minus_24hrs(self):
        vvs = VirtualVolume.query.limit(10)
        for vv in vvs:
            rslt = vv.summarise(now_minus_24hrs, now)
            self.assertTrue(isinstance(rslt, list))
            if rslt:
                self.assertIsNot(rslt, {})
                for usage in rslt:
                    self.assertIsNotNone(usage)

    def test_with_owners(self):
        # This is the only special one, hard coded until a better one found
        vv = VirtualVolume.query.get('1f6f5fce-67e0-45b2-b157-68ae7f707c22')
        if vv:
            rslt = vv.summarise(now_minus_24hrs, now)
            self.assertTrue(isinstance(rslt, list))
            for usage in rslt:
                self.assertTrue(isinstance(usage, dict))


class FSUSummaryTestCase(unittest.TestCase):
    """FilesystemUsage summarise method"""
    def test_now(self):
        rslt = FilesystemUsage.summarise(now)
        self.assertTrue(isinstance(rslt, list))

    def test_now_minus_24hrs(self):
        rslt = FilesystemUsage.summarise(now_minus_24hrs, now)
        self.assertTrue(isinstance(rslt, list))


class VVUSummaryTestCase(unittest.TestCase):
    """VirtualVolumeUsage summarise method"""
    def test_now(self):
        rslt = VirtualVolumeUsage.summarise(now)
        self.assertTrue(isinstance(rslt, list))

    def test_now_minus_24hrs(self):
        rslt = VirtualVolumeUsage.summarise(now_minus_24hrs, now)
        self.assertTrue(isinstance(rslt, list))

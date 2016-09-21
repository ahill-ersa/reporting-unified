import time
import datetime
import unittest

from ...models.xfs import Filesystem, Usage, Owner
from ...tests import now, now_minus_24hrs


class FilesystemTestCase(unittest.TestCase):
    def setUp(self):
        self.fs = Filesystem.query.first()

    def test_now(self):
        rslt = self.fs.summarise(now)
        self.assertEqual(len(rslt), 0)

    def test_now_minus_24hrs(self):
        rslt = self.fs.summarise(now_minus_24hrs, now)
        self.assertGreaterEqual(len(rslt), 0)

    def test_list_now_minus_24hrs(self):
        rslt = self.fs.list(now_minus_24hrs, now)
        for rs in rslt:
            print(rs)
        self.assertGreaterEqual(len(rslt), 0)


class UsageSummaryTestCase(unittest.TestCase):
    def test_now(self):
        rslt = Usage.summarise(now)
        self.assertEqual(len(rslt), 0)

    def test_now_minus_24hrs(self):
        rslt = Usage.summarise(now_minus_24hrs, now)
        self.assertGreaterEqual(len(rslt), 0)


class OwnerTestCase(unittest.TestCase):
    def setUp(self):
        self.owners = Owner.query.limit(10)

    def test_file_systems(self):
        old = None
        for owner in self.owners:
            fs = owner._get_file_systems()
            self.assertTrue(isinstance(fs, dict))
            self.assertGreaterEqual(len(fs.keys()), 0)
            if old:
                self.assertEqual(old, fs)
            else:
                old = fs

    def test_now(self):
        for owner in self.owners:
            rslt = owner.summarise(now)
            self.assertEqual(len(rslt), 0)

    def test_now_minus_24hrs(self):
        print(now_minus_24hrs, now)
        for owner in self.owners:
            print(owner.id)
            rslt = owner.summarise(now_minus_24hrs, now)
            print(rslt)
            self.assertGreaterEqual(len(rslt), 0)

    def test_local_remote(self):
        # try to find the cut off of days to go remote
        print(now_minus_24hrs, now)
        day_in_seconds = datetime.timedelta(days=1).total_seconds()
        owner = self.owners[3]
        for d in range(0, 30, 5):
            delta = int(d * day_in_seconds / 10)
            back_day = now - delta
            print('%0.1f, delta = %d, back_day = %d, now = %d' % (d / 10, delta, back_day, now))
            start = time.time()
            rslt = owner.summarise(back_day, now)
            # print(rslt)
            end = time.time()
            print('Run time count: %f seconds' % (end - start))

        for d in range(3, 30):
            delta = d * day_in_seconds
            back_day = now - delta
            print('%d, delta = %d, back_day = %d, now = %d' % (d, delta, back_day, now))
            start = time.time()
            rslt = owner.summarise(back_day, now)
            # print(rslt)
            end = time.time()
            print('Run time count: %f seconds' % (end - start))

    def test_list_now_minus_24hrs(self):
        for owner in self.owners:
            print(owner.name)
            rslt = owner.list(now_minus_24hrs, now)
            self.assertTrue(isinstance(rslt, dict))
            print(rslt)

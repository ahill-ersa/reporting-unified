import unittest

from ...models.hpc import Owner, Job, Host, Allocation
from ...tests import now, now_minus_24hrs


class OwnerTestCase(unittest.TestCase):
    def test_summarise_now_minus_24hrs(self):
        print(now_minus_24hrs, now)
        owners = Owner.query.all()
        for owner in owners:
            rslt = owner.summarise(now_minus_24hrs, now)
            self.assertGreaterEqual(len(rslt), 0)
            if rslt:
                print(owner.name)
                print(rslt)


class JobTestCase(unittest.TestCase):
    def test_summarise_now(self):
        rslt = Job.summarise(now)
        self.assertEqual(len(rslt), 0)

    def test_summarise_now_minus_24hrs(self):
        print(now_minus_24hrs, now)
        rslt = Job.summarise(now_minus_24hrs, now)
        self.assertGreaterEqual(len(rslt), 0)
        print(rslt)

    def test_list_now_minus_24hrs(self):
        print(now_minus_24hrs, now)
        rslt = Job.list(now_minus_24hrs, now)
        self.assertGreaterEqual(len(rslt), 0)
        print(rslt)


class HostTestCase(unittest.TestCase):
    def test_summarise_now_minus_24hrs(self):
        print(now_minus_24hrs, now)
        hosts = Host.query.all()
        for host in hosts:
            rslt = host.summarise(now_minus_24hrs, now)
            self.assertTrue(isinstance(rslt, dict))
            if rslt:
                print(host.name)
                print(rslt)


class AllocationTestCase(unittest.TestCase):
    def test_summarise_now_minus_24hrs(self):
        print(now_minus_24hrs, now)
        rslt = Allocation.summarise(now_minus_24hrs, now)
        self.assertGreaterEqual(len(rslt), 0)
        print(rslt)

    def test_summarise_runtime_now_minus_24hrs(self):
        print(now_minus_24hrs, now)
        rslt = Allocation.summarise_runtime(now_minus_24hrs, now)
        self.assertGreaterEqual(len(rslt), 0)
        print(rslt)

import unittest

from ...models.hcp import Usage, Tenant
from ...tests import now, now_minus_24hrs


class TenantTestCase(unittest.TestCase):
    def test_now(self):
        tn = Tenant.query.first()
        rslt = tn.summarise(now)
        self.assertEqual(len(rslt), 0)

    def test_now_minus_24hrs(self):
        tns = Tenant.query.all()
        for tn in tns:
            print(tn.name)
            rslt = tn.summarise(now_minus_24hrs, now)
            self.assertGreaterEqual(len(rslt), 0)
            print(rslt)


class UsageSummaryTestCase(unittest.TestCase):
    def test_now(self):
        rslt = Usage.summarise(now)
        self.assertEqual(len(rslt), 0)

    def test_now_minus_24hrs(self):
        rslt = Usage.summarise(now_minus_24hrs, now)
        self.assertGreaterEqual(len(rslt), 0)
        print(rslt)

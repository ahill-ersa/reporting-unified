import unittest

from ...models.swift import Usage
from ...tests import now, now_minus_24hrs


class UsageSummaryTestCase(unittest.TestCase):
    def test_now(self):
        rslt = Usage.summarise(now)
        self.assertTrue(isinstance(rslt, list))

    def test_now_minus_24hrs(self):
        rslt = Usage.summarise(now_minus_24hrs, now)
        self.assertTrue(isinstance(rslt, list))

    def test_until_now(self):
        rslt = Usage.summarise(end_ts=now)
        self.assertTrue(isinstance(rslt, list))

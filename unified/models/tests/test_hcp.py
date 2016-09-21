import random
import unittest

from ...models.hcp import Usage, Tenant, Namespace
from ...tests import now, now_minus_24hrs


class TenantTestCase(unittest.TestCase):
    def test_summarise_now(self):
        tn = Tenant.query.first()
        rslt = tn.summarise(now)
        self.assertEqual(len(rslt), 0)

    def test_summarise_now_minus_24hrs(self):
        tns = Tenant.query.all()
        for tn in tns:
            print(tn.name)
            rslt = tn.summarise(now_minus_24hrs, now)
            self.assertGreaterEqual(len(rslt), 0)
            print(rslt)

    def test_list_now_minus_24hrs(self):
        print(now_minus_24hrs, now)
        tns = Tenant.query.all()
        selected = random.randrange(len(tns))
        print(tns[selected].name)
        rslt = tns[selected].list(now_minus_24hrs, now)
        self.assertTrue(isinstance(rslt, dict))
        print(rslt)

    def test_summarise_total_now_minus_24hrs(self):
        # Verify some key attributes of namespaces are identical
        # to the values of those in __total__ namespace
        print(now_minus_24hrs, now)
        tns = Tenant.query.all()
        key_items = ('objects', 'ingested_bytes', 'raw_bytes', 'deletes')
        for selected in range(len(tns)):
            print(tns[selected].name)
            rslt = tns[selected].summarise(now_minus_24hrs, now)
            print(rslt)
            nss = []
            for ns in rslt:
                ns_name = ns.pop('namespace')
                if ns_name == '__total__':
                    total = ns
                else:
                    nss.append(ns)
            if len(nss):
                print(total)
                for ns in nss:
                    for k in total:
                        total[k] = total[k] - ns[k]
                self.assertTrue(all(total[checking] == 0 for checking in key_items))

class UsageSummaryTestCase(unittest.TestCase):
    def test_now(self):
        rslt = Usage.summarise(now)
        self.assertEqual(len(rslt), 0)

    def test_now_minus_24hrs(self):
        rslt = Usage.summarise(now_minus_24hrs, now)
        self.assertGreaterEqual(len(rslt), 0)
        print(rslt)


class NamespaceTestCase(unittest.TestCase):
    def test_list_now_minus_24hrs(self):
        print(now_minus_24hrs, now)
        nss = Namespace.query.all()
        selected = random.randrange(len(nss))
        print(nss[selected].name)
        rslt = nss[selected].list(now_minus_24hrs, now)
        print(rslt)
        self.assertTrue(isinstance(rslt, list))

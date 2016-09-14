import unittest
from flask import json

from ..apis.hpc import app
from . import client_get, now, now_minus_24hrs

get = client_get(app)


class HPCTestCase(unittest.TestCase):
    def test_root_not_allowed(self):
        rv = get('/')
        self.assertEqual(rv.status_code, 404)

    def test_all_top_objects_should_pass(self):
        for route in app.url_map.iter_rules():
            rule = route.rule
            # top objects' have pattern of /blar
            # ingest only accept PUT and OPTIONS
            if rule not in ('/static/<path:filename>', '/ingest') and 'summary' not in rule:
                print('Testing %s' % rule)
                resp = get('%s?count=10' % rule)
                data = json.loads(resp.data)
                self.assertEqual(resp.status_code, 200)
                self.assertGreaterEqual(len(data), 1)

    def test_summaries(self):
        summary_types = ['allocation', 'allocation/runtime', 'job']
        for stype in summary_types:
            resp = get('/%s/summary?start=%s&end=%s' % (stype, now_minus_24hrs, now))
            self.assertEqual(resp.status_code, 200)
            data = json.loads(resp.data)
            self.assertTrue(isinstance(data, list))
            print(data)

    def test_instance_summaries(self):
        instance_types = ['host', 'owner']
        for itype in instance_types:
            resp = get('/%s?count=1' % itype)
            self.assertEqual(resp.status_code, 200)
            data = json.loads(resp.data)
            self.assertTrue(isinstance(data, list))
            self.assertGreater(len(data), 0)
            print(data)

            resp = get('/%s/%s/summary?start=%s&end=%s' % (itype, data[0]['id'], now_minus_24hrs, now))
            self.assertEqual(resp.status_code, 200)
            data = json.loads(resp.data)
            print(data)

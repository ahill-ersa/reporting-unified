import unittest
from flask import json

from ..apis.hnas import app
from . import client_get, now, now_minus_24hrs

get = client_get(app)


class HNASTestCase(unittest.TestCase):
    def test_root_not_allowed(self):
        rv = get('/')
        self.assertEqual(rv.status_code, 404)

    def test_all_top_objects_should_pass(self):
        for route in app.url_map.iter_rules():
            rule = route.rule
            # top objects' have pattern of /blar
            # ingest only accept PUT and OPTIONS
            if rule not in ('/static/<path:filename>', '/ingest') and \
                    all(t not in rule for t in ('list', 'summary')):
                print('Testing %s' % rule)
                resp = get('%s?count=10' % rule)
                data = json.loads(resp.data)
                self.assertEqual(resp.status_code, 200)
                self.assertGreaterEqual(len(data), 1)

    def test_type_summary(self):
        summary_types = ('filesystem/usage', 'virtual-volume/usage')
        for stype in summary_types:
            resp = get('/%s/summary?start=%s&end=%s' % (stype, now_minus_24hrs, now))
            self.assertEqual(resp.status_code, 200)
            data = json.loads(resp.data)
            self.assertTrue(isinstance(data, list))
            print(data)

    def test_instance_methods(self):
        instance_types = ('filesystem', 'virtual-volume')
        methods = ('summary', 'list')
        for itype in instance_types:
            print('Testing type: %s' % itype)
            resp = get('/%s?count=1' % itype)
            self.assertEqual(resp.status_code, 200)
            data = json.loads(resp.data)
            print(data)
            self.assertTrue(isinstance(data, list))
            self.assertGreater(len(data), 0)
            target_id = data[0]['id']

            for method in methods:
                print('\t method: %s' % method)
                resp = get('/%s/%s/%s?start=%s&end=%s' % (itype, target_id, method, now_minus_24hrs, now))
                self.assertEqual(resp.status_code, 200)
                data = json.loads(resp.data)
                print(data)
                self.assertTrue(isinstance(data, list) or isinstance(data, dict))
                self.assertGreater(len(data), 0)

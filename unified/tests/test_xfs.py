import unittest
from flask import json

from ..apis.xfs import app
from . import client_get, now, now_minus_24hrs

get = client_get(app)


class XFSTestCase(unittest.TestCase):
    def test_root_not_allowed(self):
        rv = get('/')
        self.assertEqual(rv.status_code, 404)

    def test_all_top_objects_should_pass(self):
        for route in app.url_map.iter_rules():
            rule = route.rule
            # top objects' have pattern of /blar
            # ingest only accept PUT and OPTIONS
            if rule not in ('/static/<path:filename>', '/ingest') and 'summary' not in rule and 'list' not in rule:
                print('Testing %s' % rule)
                resp = get('%s?count=10' % rule)
                data = json.loads(resp.data)
                self.assertEqual(resp.status_code, 200)
                self.assertGreaterEqual(len(data), 1)

    def test_filesystem_not_found(self):
        rule = '/filesystem/not/summary'
        # Can deal non-uuid id quitely
        resp = get(rule)
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(len(data), 0)

        rule = '/filesystem/12345678123456781234567812345678/summary'
        resp = get(rule)
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(len(data), 0)

    def test_usage_summary(self):
        resp = get('/usage/summary?start=%s&end=%s' % (now_minus_24hrs, now))
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertTrue(isinstance(data, list))
        print(data)

    def test_instance_methods(self):
        instance_types = ('filesystem', 'owner')
        methods = ('summary', 'list')
        for itype in instance_types:
            resp = get('/%s?count=1' % itype)
            self.assertEqual(resp.status_code, 200)
            data = json.loads(resp.data)
            print(data)
            self.assertTrue(isinstance(data, list))
            self.assertGreater(len(data), 0)
            target_id = data[0]['id']

            for method in methods:
                resp = get('/%s/%s/%s?start=%s&end=%s' % (itype, target_id, method, now_minus_24hrs, now))
                self.assertEqual(resp.status_code, 200)
                data = json.loads(resp.data)
                print(data)
                self.assertTrue(isinstance(data, list) or isinstance(data, dict))
                self.assertGreater(len(data), 0)

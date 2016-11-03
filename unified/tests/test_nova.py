import unittest
from flask import json

from ..apis.nova import app
from . import client_get

get = client_get(app)


class NovaTestCase(unittest.TestCase):
    def test_root_not_allowed(self):
        rv = get('/')
        self.assertEqual(rv.status_code, 404)

    def test_all_top_objects_should_pass(self):
        for route in app.url_map.iter_rules():
            rule = route.rule
            # top objects' have pattern of /blar
            # ingest only accept PUT and OPTIONS
            if rule not in ('/static/<path:filename>', '/ingest', '/instance', '/summary', '/instance/<id>/latest'):
                print('Testing %s' % rule)
                resp = get('%s?count=10' % rule)
                data = json.loads(resp.data)
                self.assertEqual(resp.status_code, 200)
                self.assertGreaterEqual(len(data), 1)

    def test_summary_needs_start_end(self):
        rule = '/summary'
        resp = get(rule)
        self.assertEqual(resp.status_code, 400)

        resp = get('%s?start=123' % rule)
        self.assertEqual(resp.status_code, 400)

        resp = get('%s?end=123' % rule)
        self.assertEqual(resp.status_code, 400)

    def test_summary(self):
        rule = '/summary'
        resp = get('%s?start=123&end=123' % rule)
        self.assertEqual(resp.status_code, 200)
        resp_data = json.loads(resp.data)
        self.assertEqual(resp_data, {'items': [], 'page': 1, 'pages': 0, 'total': 0})

    def test_latest_state(self):
        resp = get('/flavor?count=1')
        flavor_id = json.loads(resp.data)[0]['id']
        resp = get('/instance?count=1&filter=flavor_id.eq.' + flavor_id)
        instance_id = json.loads(resp.data)[0]['id']
        resp = get('/instance/%s/latest' % instance_id)
        self.assertEqual(resp.status_code, 200)
        resp_data = json.loads(resp.data)
        self.assertEqual(resp_data['instance_id'], instance_id)

import unittest
from flask import json

from ..apis.swift import app
from . import client_get, now, now_minus_24hrs

get = client_get(app)


class SwiftTestCase(unittest.TestCase):
    def test_root_not_allowed(self):
        rv = get('/')
        self.assertEqual(rv.status_code, 404)

    def test_all_top_objects_should_pass(self):
        for route in app.url_map.iter_rules():
            rule = route.rule
            # top objects' have pattern of /blar
            # ingest only accept PUT and OPTIONS
            if rule not in ('/static/<path:filename>', '/ingest'):
                print('Testing %s' % rule)
                resp = get('%s?count=10' % rule)
                data = json.loads(resp.data)
                self.assertEqual(resp.status_code, 200)
                self.assertGreaterEqual(len(data), 1)

    def test_usage_summary(self):
        resp = get('/usage/summary?start=%s&end=%s' % (now_minus_24hrs, now))
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertTrue(isinstance(data, list))
        print(data)

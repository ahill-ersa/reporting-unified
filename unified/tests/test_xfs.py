import unittest
from flask import json

from ..apis.xfs import app
from . import client_get

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
            if rule not in ('/static/<path:filename>', '/ingest', '/snapshot/summary', '/filesystem/<id>/summary'):
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

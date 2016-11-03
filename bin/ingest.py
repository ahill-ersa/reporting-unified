#!/usr/bin/env python3

# pylint: disable=C0111,W0212,W0611,E1102,W0703

"""
Ingestion Tool

Modified from ersa-reporting-ingest
"""

import base64
import hashlib
import json
import lzma
import os
import random
import time
import requests
import concurrent.futures

from argparse import ArgumentParser
from sys import exit
import logging

from boto.s3.connection import S3Connection

# HCP #facepalm
import ssl
if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context

logger = logging.getLogger('reporting-ingest')

DEBUG = True

# TODO: move to another place for better sharing with other packages
class HCP:
    def __init__(self, aws_id, aws_secret, server, bucket):
        aws_id = base64.b64encode(bytes(aws_id, "utf-8")).decode()
        aws_secret = hashlib.md5(bytes(aws_secret, "utf-8")).hexdigest()
        hs3 = S3Connection(aws_access_key_id=aws_id,
                           aws_secret_access_key=aws_secret,
                           host=server)
        self.bucket = hs3.get_bucket(bucket)

    def exists(self, name):
        return name in self.bucket

    def put(self, name, data):
        self.bucket.new_key(name).set_contents_from_string(data)

    def get(self, name):
        return self.bucket.get_key(name,
                                   validate=False).get_contents_as_string()

    def items(self, prefix=None):
        return self.bucket.list(prefix=prefix)


class Ingester:
    """
      Ingest from object store or from local files

      From object store, it compares the list of objects in store and entries in
      input table of database. It can be slow.

      From local: upload a list of local files
    """
    def __init__(self, conf):
        try:
            self.endpoint = conf['DB_API']['ENDPOINT']
            self.token = conf['DB_API']['TOKEN']
            self.schema = conf['DB_API'].get('SCHEMA', '')

            store_id = conf['HCP']['ID']
            store_secret = conf['HCP']['SECRET']
            store_url = conf['HCP']['ENDPOINT']
            bucket = conf['HCP']['BUCKET']
        except Exception as e:
            raise KeyError("Configuration key error: %s" % str(e))

        self.prefix = conf['HCP'].get('PREFIX', '')
        self.substring = conf['HCP'].get('SUBSTRING', '')

        try:
            self.hcp = HCP(store_id, store_secret, store_url, bucket)
        except Exception:
            raise ConnectionError("Cannot connect object store.")

        logger.debug('Ingest from store prefix %s into %s' % (self.prefix, self.endpoint))

    def _make_request(self, query):
        url = "%s/%s" % (self.endpoint, query)
        return requests.get(url, headers={"x-ersa-auth-token": self.token})

    def _verify_exist(self, rst):
        """Check the response for verifying existence"""
        if rst.status_code == 204:
            # ingested successfully will receive 204 not 200
            return True
        elif rst.status_code == 200:
            return len(rst.json()) > 0
        else:
            logger.error("HTTP error %d" % rst.status_code)
            return False

    def check_input(self, name):
        query = "input?filter=name.eq.%s" % name
        logger.debug(query)
        rst = self._make_request(query)
        return self._verify_exist(rst)

    def _put(self, name, data):
        return requests.put("%s/ingest?name=%s" % (self.endpoint, name),
                      headers={
                          "content-type": "application/json",
                          "x-ersa-auth-token": self.token
                      },
                      data=json.dumps(data))

    def fetch(self, name):
        logger.debug("Retrieve and decompress %s from HCP" % name)
        return json.loads(lzma.decompress(self.hcp.get(name)).decode("utf-8"))

    def list_ingested(self):
        """List the ingested messages files in input table of database through API server"""
        page = 1
        names = []

        while True:
            url = "%s/input?count=5000&page=%s" % (self.endpoint, page)
            batch = requests.get(url, headers={"x-ersa-auth-token": self.token})
            # This is for back compatibility
            if batch.status_code == 404:
                break
            elif batch.status_code != 200:
                raise IOError("HTTP %s" % batch.status_code)

            # batch.status_code == 200 but with empty result
            records = batch.json()
            if len(records) > 0:
                names += [item["name"] for item in batch.json()]
                logger.debug("%d page loaded" % page)
                page += 1
            else:
                break

        return names

    def _prepare_batch_list(self):
        """Query input table and process json.xz in hcp which have not been ingested"""
        # The list can be very long if prefix is not used
        logger.debug("Getting list of archived packages of messages from database through API")
        ingested = self.list_ingested()
        ingested= set(ingested)

        logger.debug("Get list of archived packages of messages from object store")
        all_items = [item.name
             for item in self.hcp.items(prefix=self.prefix)
             if not item.name.endswith("/")]

        if self.substring:
            all_items = [item for item in all_items if self.substring in item]

        all_items = set(all_items)

        todo = list(all_items - ingested)

        logger.info("%s objects, %s already ingested, %s todo" %
              (len(all_items), len(ingested.intersection(all_items)), len(todo)))

        return todo

    def batch(self):
        # end point is defined in config json file
        # As individual job is registered in input, set comparison should be avoid when numbers are too high
        logger.debug("Preparing list")
        todo = self._prepare_batch_list()
        for name in todo:
            logger.debug(name)
            data = self.fetch(name)
            if self.schema:
                data = [item for item in data if item["schema"] == self.schema]

            tracking_name = name

            success = self._verify_exist(self._put(tracking_name, data))
            if not success:
                logger.error("%s was not ingested" % name)

    def _log_put(self, tracking_name, data):
        success = self._verify_exist(self._put(tracking_name, data))
        if not success:
            logger.error("%s was not ingested" % tracking_name)

    def put_single_xz(self, xz_name, input_name):
        # This deal with local files for debug purpose, it will ingest each message separately in a xz file
        with lzma.LZMAFile(xz_name) as f:
            data = json.loads(f.read().decode("utf-8"))

        self._log_put(input_name, data)

    def put_local_xz(self, xz_name, input_name, check=False):
        # By default not check if exist as it can be very slow
        if check and self.check_input(input_name):
            logger.debug("Cannot ingest %s because it has been ingested before as the name %s is found in database." % (xz_name, input_name))
        else:
            self.put_single_xz(xz_name, input_name)

def read_conf(path):
    # Check that the configuration file exists
    if not os.path.isfile(path):
        raise Exception("Config file %s cannot be found" % path)

    with open(path, 'r') as f:
        conf = json.load(f)
    return conf

def parse_command(description='Ingest records from HCP to Database through API server'):
    parser = ArgumentParser(description=description)
    parser.add_argument('conf', default='config.json',
        help='Path to config.json. Default = config.json')
    args = parser.parse_args()
    return read_conf(args.conf)

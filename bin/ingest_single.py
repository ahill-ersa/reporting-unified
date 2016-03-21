#!/usr/bin/env python3

"""Ingeset a file downloaded to database through API.
   collector_cluster_name/topic/parition/json.xz.file
"""

import sys
import logging
from argparse import ArgumentParser

from ingest import Ingester, read_conf

def parse_command(description='Ingest records from HCP to Database through API server'):
    parser = ArgumentParser(description=description)
    parser.add_argument('name', help='Path to the json.xz file to be ingested')
    parser.add_argument('-t', '--tracker', default='', help='A string, commonly name of an object in store')
    parser.add_argument('--conf', default='debugger_api_conf.json', help='Path to config.json. Default = debugger_api_conf.json')
    args = parser.parse_args()
    if args.tracker:
        return args.name, args.tracker, args.conf
    else:
        return args.name, args.name, args.conf

if __name__ == "__main__":
    #logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    logging.basicConfig(format='%(levelname)s:%(message)s')

    # It only needs DB_API of a normal conf file: API end point, secret and optional schema
    # But currently Ingester is not flexible for it.
    try:
        file_name, tracker, conf_file = parse_command('Ingest records from HCP to Database through API server')
    except Exception as e:
        logging.error(e)
        sys.exit(2)

    logging.debug("Ingesting %s tracked as %s with connection conf in %s" % (file_name, tracker, conf_file))
    conf = read_conf(conf_file)

    ingester = Ingester(conf)
    #logging.debug(ingester.check_input(tracker))
    ingester.put_local_xz(file_name, tracker, True)

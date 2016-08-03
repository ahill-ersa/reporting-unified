""" An utility script for calculating usages of services """


import os
import sys
import json
import inspect
import logging
import datetime

from argparse import ArgumentParser

import usage.types


logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.DEBUG)
logger = logging.getLogger()
# Stop upstream libraries polluting our debug logger
logging.getLogger("requests").setLevel(logging.WARNING)

DAY_IN_SECS = 86399  # 24 * 60 * 60 - 1


def parse_date_string(date_string):
    return int(datetime.datetime.strptime(date_string, '%Y%m%d').timestamp())


def read_conf(path):
    # Check that the configuration file exists
    if not os.path.isfile(path):
        raise Exception("Config file %s cannot be found" % path)

    with open(path, 'r') as f:
        conf = json.load(f)
    return conf


def get_links():
    return []


class Calculator(object):
    def __init__(self, calculator, start, end):
        self.calculator = calculator

    def calculate(self):
        # calculate usage
        usages = self.calculator.calculate()
        return usages

    def link(self, usages):
        # get organisation, school, manager and link them to each calculated result
        for u in usages:
            links = get_links(u.contractor.id)
            u.update(links)
        pass

    def save(self, file_name, usages):
        # save to a file in json format
        with open(file_name, 'w') as jf:
            json.dump(usages, jf)


if __name__ == '__main__':
    parser = ArgumentParser(description='Calculate usage of a service in a time interval.')
    parser.add_argument('service', choices=['nova', 'hpc', 'xfs'], help='Name of service')
    parser.add_argument('-s', '--start', required=True, help='Start date of the interval')
    parser.add_argument('-e', '--end', required=True, help='End date of the interval')
    parser.add_argument('--conf', default='config.json',
                        help='Path to config.json. Default = config.json')

    args = parser.parse_args()

    service = args.service.capitalize()
    start_date = args.start
    end_date = args.end

    try:
        min_date = parse_date_string(start_date)
    except Exception as e:
        print("Wrong date string: %s" % str(e))
        sys.exit(0)

    try:
        max_date = parse_date_string(end_date) + DAY_IN_SECS
    except Exception as e:
        print("Wrong date string: %s" % str(e))
        sys.exit(0)

    logger.debug("Usage of %s in %d - %d" % (service, min_date, max_date))
    config = read_conf(args.conf)

    import time
    start = time.time()

    cal_class = getattr(usage.types, service + 'Usage')
    cal = cal_class(min_date, max_date, **config[service])
    cal.calculate()

    end = time.time()
    logger.debug('Run time count: %d' % (end - start))

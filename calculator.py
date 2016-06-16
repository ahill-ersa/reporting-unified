""" An utility script for calculating usages of services """


import sys
import json
import logging
import requests
import datetime


from argparse import ArgumentParser

import ersa_reporting.models


logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.DEBUG)
logger = logging.getLogger()
# Stop upstream libraries polluting our debug logger
logging.getLogger("requests").setLevel(logging.WARNING)

DAY_IN_SECS = 86399  # 24 * 60 * 60 - 1


def parse_date_string(date_string):
    return int(datetime.datetime.strptime(date_string, '%Y%m%d').timestamp())


class Users(object):
    """ User relationship query through some source's RESTful APIs"""

    def __init__(self, url, token=None):
        self.end_point = url

    def get(self):
        j = requests.get(self.end_point).json()
        logger.debug(j)


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
        for usage in usages:
            links = get_links(usage.contractor.id)
            usage.update(links)
        pass

    def save(self, file_name, usages):
        # save to a file in json format
        with open(file_name, 'w') as jf:
            json.dump(usages, jf)


if __name__ == '__main__':
    parser = ArgumentParser(description='Calculate usage of a service in a time interval.')
    parser.add_argument('service', help='Name of service')
    parser.add_argument('-s', '--start', required=True, help='Start date of the interval')
    parser.add_argument('-e', '--end', required=True, help='End date of the interval')

    args = parser.parse_args()

    service = args.service
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

    user_links = Users('http://144.6.236.232/bman/api/person/1/')
    user_links.get()

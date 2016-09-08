""" An utility script for calculating usages of services """


import os
import sys
import json
import time
import inspect
import datetime
import calendar

import logging
import logging.handlers


from argparse import ArgumentParser


DAY_IN_SECS = 86399  # 24 * 60 * 60 - 1


def parse_date_string(date_string):
    return int(datetime.datetime.strptime(date_string, '%Y%m%d').timestamp())


def month_ends(year, month):
    """Get day strings of start and end of the year and the month"""
    def _last_day(year, month):
        """Get the last day in the year and the month"""
        assert month > 0 and month < 13, 'Month must be in 1..12'
        return max(calendar.Calendar().itermonthdays(year, month))

    if isinstance(month, str):
        month = int(month)
    return '%d%d%d' % (year, month, 1), \
           '%d%d%d' % (year, month, _last_day(year, month))


def read_conf(path):
    # Check that the configuration file exists
    if not os.path.isfile(path):
        raise Exception('Config file %s cannot be found' % path)

    with open(path, 'r') as f:
        conf = json.load(f)
    return conf


def set_logger(config):
    """Sets up logger for packages"""
    LOG_FORMAT = '%(asctime)s %(levelname)s %(lineno)d: %(message)s'
    SAN_MS_DATE = '%Y-%m-%d %H:%M:%S'
    LOG_FORMATTER = logging.Formatter(LOG_FORMAT, SAN_MS_DATE)

    # As we set up root logger, stop other libraries polluting our logger
    logging.getLogger("requests").setLevel(logging.WARNING)

    if 'LOG' in config:
        # We need to log to file
        logger = logging.getLogger()
        fh = logging.handlers.RotatingFileHandler(config['LOG']['PATH'], maxBytes=config['LOG'].get('SIZE', 0))
        fh.setFormatter(LOG_FORMATTER)
        logger.addHandler(fh)
        logger.setLevel(config['LOG'].get('LEVEL', 'INFO'))
    else:
        logging.basicConfig(format=LOG_FORMAT, datefmt=SAN_MS_DATE, level=logging.DEBUG)
    logging.debug("Logger set up finished")


if __name__ == '__main__':
    parser = ArgumentParser(description='Calculate usage of a service in a time interval.')
    parser.add_argument('service', choices=['nova', 'hpc', 'xfs'], help='Name of service')
    parser.add_argument('-y', '--year', help='Year of monthly bill to be generated')
    parser.add_argument('-m', '--month', help='Month of monthly bill to be generated')
    parser.add_argument('-s', '--start', help='Start date (%Y%m%d) of the interval')
    parser.add_argument('-e', '--end', help='End date (%Y%m%d) of the interval')
    parser.add_argument('--conf', default='config.json',
                        help='Path to configuration JSON file. Default = config.json')

    args = parser.parse_args()

    service = args.service.capitalize()
    if args.month:
        print(args.month)
        year = int(args.year) if args.year else datetime.date.today().year
        start_date, end_date = month_ends(year, args.month)
    else:
        if args.start is None or args.end is None:
            print("Date range mode needs both start and end dates in %Y%m%d format")
            sys.exit(1)
        start_date = args.start
        end_date = args.end

    try:
        min_date = parse_date_string(start_date)
    except Exception as e:
        print("Not a valid date string: %s" % str(e))
        sys.exit(1)

    try:
        max_date = parse_date_string(end_date) + DAY_IN_SECS
    except Exception as e:
        print("Not a valid date string: %s" % str(e))
        sys.exit(1)

    config = read_conf(args.conf)
    os.environ['APP_SETTINGS'] = config[service].pop('APP')
    set_logger(config)

    logging.info("Start to calculate the usage of %s in %d - %d" % (service, min_date, max_date))

    start = time.time()

    import usage.types
    cal_class = getattr(usage.types, service + 'Usage')
    if service in config:
        crm_conf = config[service].pop('crm')
        client = usage.types.BmanClient(**crm_conf)
        cal = cal_class(min_date, max_date, client, **config[service])
    else:
        # Not really useful
        cal = cal_class(min_date, max_date)
    saved_fname = cal.calculate()
    logging.info("Calculated usages can be found in %s" % saved_fname)

    end = time.time()
    logging.debug('Run time count: %d seconds' % (end - start))

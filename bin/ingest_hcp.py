#!/usr/bin/env python3

import sys
import logging

from ingest import parse_command, Ingester

logger = logging.getLogger('reporting-ingest')

if __name__ == "__main__":
    try:
        conf = parse_command('Ingest records from HCP to Database through API server')
    except Exception as e:
        logger.error(e)
        sys.exit(2)

    end_point = conf['DB_API']['ENDPOINT']
    log_file = "%s.log" % end_point[end_point.index('/')+2:].replace('/', '_')
    # In case there is a / at the end of end point
    log_file = log_file.replace('_.log', '.log')

    file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=30000000)
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

    logger.debug('Start an ingest job')

    ingester = Ingester(conf)
    ingester.batch()
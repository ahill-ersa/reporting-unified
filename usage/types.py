import logging
import requests
import concurrent.futures
from urllib.parse import urlencode

from ersa_reporting.models.nova import Summary, Instance, Flavor  # noqa: E402


logger = logging.getLogger(__name__)


class Client(object):
    """RESTful APIs' client"""

    def __init__(self, url, token=None):
        self.end_point = url
        self.headers = None
        if token:
            self.headers = {'x-ersa-auth-token': token}

    def group(self, ids, size=10):
        """Slice uuids into managable chunks for optimising request performance"""
        return [ids[i:i + size] for i in range(0, len(ids), size)]

    def _verify(self, rst):
        """Check the response for verifying existence"""
        if rst.status_code < 300:
            return True
        else:
            logger.error("Request to %s failed. HTTP error code = %d" % (self.end_point, rst.status_code))
            return False

    def get(self, path='', args={}):
        url = self.end_point + path

        query_string = urlencode(args)
        url = url + '?' + query_string if query_string else self.end_point
        logger.debug(url)

        req = requests.get(url, headers=self.headers)
        if self._verify(req):
            j = req.json()
            logger.debug(j)
            return j

        return None


class Usage(object):
    # It needs to know how to get data, score usage
    def __init__(self, start, end, **kwargs):
        # conditions are used to define how to prepare data
        # source, model.
        # Derived classes may have their own set up conditions:
        # usage model, prices(?), etc
        self.start_timestamp = start
        self.end_timestamp = end
        logger.debug("Query arguments: start=%s, end=%s" % (self.start_timestamp, self.end_timestamp))

    def prepare(self):
        # get data from source, do other preparations
        pass

    def calculate(self):
        logger.debug("Nothing to see here")
        return []


class NovaUsage(Usage):
    def __init__(self, start, end, end_point=None, token=None, workers=1):
        super().__init__(start, end)
        assert end_point, 'End point has to be set'
        self.client = Client(end_point, token)
        self.concurrent_workers = workers

    def _get_instances(self, ids):
        id_filter = ','.join(ids)
        qargs = {'filter': 'id.in.' + id_filter}
        return self.client.get('/instance', qargs)

    def _get_state(self, instance_id):
        qargs = {'start': self.start_timestamp, 'end': self.end_timestamp, 'id': instance_id}
        return self.client.get('/instance', qargs)

    def prepare(self):
        qargs = {'distinct': True,
                 'start': self.start_timestamp,
                 'end': self.end_timestamp}
        ret = self.client.get('/summary', qargs)
        if ret['total']:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrent_workers) as executor:
                for instance_id in ret['items']:
                    # span is the key attribute we are after
                    executor.submit(self._get_state, instance_id)
                    #instance = Instance.query.get(id)
                    #state = instance.latest_state(start, end)

    def calculate(self):
        logger.debug("%s called" % self.__class__.__name__)
        self.prepare()

        return []

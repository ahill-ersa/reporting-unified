import string

from functools import lru_cache, reduce

from . import app, configure, request
from . import add, get_or_create, commit
from . import QueryResource, BaseIngestResource

from ..models.swift import (
    Snapshot, Account, Usage)


class SnapshotResource(QueryResource):
    query_class = Snapshot


class AccountResource(QueryResource):
    query_class = Account


class UsageResource(QueryResource):
    query_class = Usage


class IngestResource(BaseIngestResource):
    def ingest(self):
        @lru_cache(maxsize=100000)
        def cache(model, **kwargs):
            return get_or_create(model, **kwargs)

        for message in request.get_json(force=True):
            data = message["data"]

            snapshot = cache(Snapshot, ts=data["timestamp"])

            for key, value in data.items():
                # Ugly hack until swift data pushed down into own dict.
                valid = [c in string.hexdigits for c in key]
                if not reduce(lambda x, y: x and y, valid):
                    continue

                account = cache(Account, openstack_id=key)

                add(Usage(bytes=value["bytes"],
                          containers=value["containers"],
                          objects=value["objects"],
                          quota=value["quota"],
                          account=account,
                          snapshot=snapshot))

        commit()

        return "", 204


def setup():
    """Let's roll."""

    resources = {
        "/snapshot": SnapshotResource,
        "/account": AccountResource,
        "/usage": UsageResource,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

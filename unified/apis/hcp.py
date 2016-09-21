import re
import uuid
import arrow

from functools import lru_cache

from . import app, configure, request, instance_method
from . import get_or_create, commit, add
from . import QueryResource, BaseIngestResource, RangeQuery

from ..models.hcp import Snapshot, Allocation, Tenant, Namespace, Usage

ALPHA_PREFIX = re.compile("^[A-Za-z]+")


class SnapshotResource(QueryResource):
    query_class = Snapshot


class AllocationResource(QueryResource):
    query_class = Allocation


class TenantResource(QueryResource):
    query_class = Tenant


class TenantSummary(RangeQuery):
    def _get(self, id='', **kwargs):
        return instance_method(Tenant, 'summarise', id,
                               start_ts=kwargs['start'],
                               end_ts=kwargs['end'])


class TenantList(RangeQuery):
    def _get(self, id='', **kwargs):
        return instance_method(Tenant, 'list', id,
                               default={},
                               start_ts=kwargs['start'],
                               end_ts=kwargs['end'])


class NamespaceResource(QueryResource):
    query_class = Namespace


class NamespaceList(RangeQuery):
    def _get(self, id='', **kwargs):
        return instance_method(Namespace, 'list', id,
                               start_ts=kwargs['start'],
                               end_ts=kwargs['end'])


class UsageResource(QueryResource):
    query_class = Usage


class UsageSummary(RangeQuery):
    def _get(self, **kwargs):
        return Usage.summarise(start_ts=kwargs['start'], end_ts=kwargs['end'])


def extract_allocation(name):
    """Check for an allocation suffix."""
    if not name.islower():
        return None
    if "-" not in name:
        return None
    name = name.split("-")
    try:
        return int(ALPHA_PREFIX.sub("", name[-1]))
    except ValueError:
        return None


class IngestResource(BaseIngestResource):
    def ingest(self):
        """Ingest usage."""

        timestamps = set()

        @lru_cache(maxsize=10000)
        def cache(model, **kwargs):
            return get_or_create(model, **kwargs)

        for message in request.get_json(force=True):
            data = message["data"]

            timestamp = data["timestamp"]
            if timestamp in timestamps:
                continue
            else:
                timestamps.add(timestamp)

            snapshot = cache(Snapshot, ts=timestamp)

            for tenant_name, namespaces in data.items():
                if not isinstance(namespaces, list):
                    continue

                allocation = None
                allocation_id = extract_allocation(tenant_name)
                if allocation_id:
                    allocation = cache(Allocation, allocation=allocation_id)

                tenant = cache(Tenant, name=tenant_name, allocation=allocation)

                for details in namespaces:
                    if "namespaceName" in details:
                        namespace_name = details["namespaceName"]
                    else:
                        namespace_name = "__total__"

                    allocation = None
                    allocation_id = extract_allocation(namespace_name)
                    if allocation_id:
                        allocation = cache(Allocation,
                                           allocation=allocation_id)

                    namespace = cache(Namespace,
                                      name=namespace_name,
                                      tenant=tenant,
                                      allocation=allocation)

                    start_time = arrow.get(details["startTime"]).timestamp
                    end_time = arrow.get(details["endTime"]).timestamp

                    usage = {
                        "snapshot": snapshot,
                        "namespace": namespace,
                        "start_time": start_time,
                        "end_time": end_time,
                        "ingested_bytes": details["ingestedVolume"],
                        "raw_bytes": details["storageCapacityUsed"],
                        "reads": details["reads"],
                        "writes": details["writes"],
                        "deletes": details["deletes"],
                        "objects": details["objectCount"],
                        "bytes_in": details["bytesIn"],
                        "bytes_out": details["bytesOut"]
                    }

                    if "metadataOnlyObjects" in details:
                        usage["metadata_only_objects"] = details["metadataOnlyObjects"]
                    else:
                        usage["metadata_only_objects"] = 0

                    if "metadataOnlyBytes" in details:
                        usage["metadata_only_bytes"] = details["metadataOnlyBytes"],
                    else:
                        usage["metadata_only_bytes"] = 0

                    if "tieredObjects" in details:
                        usage["tiered_objects"] = details["tieredObjects"],
                    else:
                        usage["tiered_objects"] = 0

                    if "tieredBytes" in details:
                        usage["tiered_bytes"] = details["tieredBytes"]
                    else:
                        usage["tiered_bytes"] = 0

                    add(Usage(**usage))

        commit()

        return "", 204


def setup():
    """Let's roll."""

    resources = {
        "/snapshot": SnapshotResource,
        "/allocation": AllocationResource,
        "/tenant": TenantResource,
        "/tenant/<id>/summary": TenantSummary,
        "/tenant/<id>/list": TenantList,
        "/namespace": NamespaceResource,
        "/namespace/<id>/list": NamespaceList,
        "/usage": UsageResource,
        "/usage/summary": UsageSummary,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

#!/usr/bin/python3
"""Application and persistence management."""

# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name

import re

import arrow

from functools import lru_cache, reduce

from ersa_reporting import db, id_column, configure, BaseIngestResource
from ersa_reporting import get_or_create, commit, app, to_dict
from ersa_reporting import add, delete, request, require_auth
from ersa_reporting import Resource, QueryResource, record_input

ALPHA_PREFIX = re.compile("^[A-Za-z]+")


class Allocation(db.Model):
    """Storage Allocation"""
    id = id_column()
    allocation = db.Column(db.Integer, unique=True, nullable=False)
    tenants = db.relationship("Tenant", backref="allocation")
    namespaces = db.relationship("Namespace", backref="allocation")

    def json(self):
        """JSON"""
        return to_dict(self, ["allocation"])


class Snapshot(db.Model):
    """Storage Snapshot"""
    id = id_column()
    ts = db.Column(db.Integer, nullable=False)
    usage = db.relationship("Usage", backref="snapshot")

    def json(self):
        """JSON"""
        return to_dict(self, ["ts"])


class Tenant(db.Model):
    """HCP Tenant"""
    id = id_column()
    name = db.Column(db.String(256), unique=True, nullable=False)
    namespaces = db.relationship("Namespace", backref="tenant")
    allocation_id = db.Column(None, db.ForeignKey("allocation.id"))

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "allocation_id"])


class Namespace(db.Model):
    """HCP Namespace"""
    id = id_column()
    name = db.Column(db.String(256), nullable=False)
    usage = db.relationship("Usage", backref="namespace")
    tenant_id = db.Column(None,
                          db.ForeignKey("tenant.id"),
                          index=True,
                          nullable=False)
    allocation_id = db.Column(None, db.ForeignKey("allocation.id"))

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "tenant_id", "allocation_id"])


class Usage(db.Model):
    """HCP Usage"""
    id = id_column()
    start_time = db.Column(db.Integer, index=True, nullable=False)
    end_time = db.Column(db.Integer, index=True, nullable=False)
    ingested_bytes = db.Column(db.BigInteger, nullable=False)
    raw_bytes = db.Column(db.BigInteger, nullable=False)
    reads = db.Column(db.BigInteger, nullable=False)
    writes = db.Column(db.BigInteger, nullable=False)
    deletes = db.Column(db.BigInteger, nullable=False)
    objects = db.Column(db.BigInteger, nullable=False)
    bytes_in = db.Column(db.BigInteger, nullable=False)
    bytes_out = db.Column(db.BigInteger, nullable=False)
    metadata_only_objects = db.Column(db.BigInteger, nullable=False)
    metadata_only_bytes = db.Column(db.BigInteger, nullable=False)
    tiered_objects = db.Column(db.BigInteger, nullable=False)
    tiered_bytes = db.Column(db.BigInteger, nullable=False)
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            index=True,
                            nullable=False)
    namespace_id = db.Column(None,
                             db.ForeignKey("namespace.id"),
                             index=True,
                             nullable=False)

    def json(self):
        """JSON"""
        return to_dict(
            self,
            ["start_time", "end_time", "ingested_bytes", "raw_bytes", "reads",
             "writes", "deletes", "objects", "bytes_in", "bytes_out",
             "metadata_only_objects", "metadata_only_bytes", "tiered_objects",
             "tiered_bytes", "snapshot_id", "namespace_id"])


class SnapshotResource(QueryResource):
    query_class = Snapshot


class AllocationResource(QueryResource):
    query_class = Allocation


class TenantResource(QueryResource):
    query_class = Tenant


class NamespaceResource(QueryResource):
    query_class = Namespace


class UsageResource(QueryResource):
    query_class = Usage


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
                        "bytes_out": details["bytesOut"],
                        "metadata_only_objects":
                        details["metadataOnlyObjects"],
                        "metadata_only_bytes": details["metadataOnlyBytes"],
                        "tiered_objects": details["tieredObjects"],
                        "tiered_bytes": details["tieredBytes"]
                    }

                    add(Usage(**usage))

        commit()

        return "", 204


def setup():
    """Let's roll."""

    resources = {
        "/snapshot": SnapshotResource,
        "/allocation": AllocationResource,
        "/tenant": TenantResource,
        "/namespace": NamespaceResource,
        "/usage": UsageResource,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

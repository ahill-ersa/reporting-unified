#!/usr/bin/python3
"""Application and persistence management."""

# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name

import string

from functools import lru_cache, reduce

from ersa_reporting import db, id_column, configure
from ersa_reporting import get_or_create, commit, app, to_dict
from ersa_reporting import add, delete, request, require_auth
from ersa_reporting import Resource, QueryResource, record_input
from ersa_reporting import BaseIngestResource


class Account(db.Model):
    id = id_column()
    openstack_id = db.Column(db.String(64), unique=True, nullable=False)
    account_snapshots = db.relationship("Usage", backref="account")

    def json(self):
        """JSON"""
        return to_dict(self, ["openstack_id"])


class Snapshot(db.Model):
    id = id_column()
    ts = db.Column(db.Integer, unique=True, nullable=False)
    account_snapshots = db.relationship("Usage", backref="snapshot")

    def json(self):
        """JSON"""
        return to_dict(self, ["ts"])


class Usage(db.Model):
    id = id_column()
    bytes = db.Column(db.BigInteger, nullable=False)
    containers = db.Column(db.Integer, nullable=False)
    objects = db.Column(db.Integer, nullable=False)
    quota = db.Column(db.BigInteger)
    account_id = db.Column(None, db.ForeignKey("account.id"), nullable=False)
    snapshot_id = db.Column(None, db.ForeignKey("snapshot.id"), nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["bytes", "containers", "objects", "quota", "account_id", "snapshot_id"])


class AccountResource(QueryResource):
    query_class = Account


class SnapshotResource(QueryResource):
    query_class = Snapshot


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

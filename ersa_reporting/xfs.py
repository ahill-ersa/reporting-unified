#!/usr/bin/env python3
"""Application and persistence management."""

# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name
# pylint: disable=no-self-use

import io
import uuid

from functools import lru_cache

from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from ersa_reporting import app, db, id_column, configure
from ersa_reporting import get, get_or_create, commit, Input
from ersa_reporting import add, request, require_auth
from ersa_reporting import QueryResource, INPUT_PARSER

# Data Models


class Owner(db.Model):
    """Storage Owner"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    usage = db.relationship("Usage", backref="owner")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class Host(db.Model):
    """Storage Host"""
    id = id_column()
    name = db.Column(db.String(256), unique=True, nullable=False)
    snapshots = db.relationship("Snapshot", backref="host")
    filesystems = db.relationship("Filesystem", backref="host")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class Snapshot(db.Model):
    """Storage Snapshot"""
    id = id_column()
    ts = db.Column(db.Integer, nullable=False)
    usage = db.relationship("Usage", backref="snapshot")
    host_id = db.Column(None, db.ForeignKey("host.id"), nullable=False)
    message = db.Column(UUID, nullable=False, unique=True)

    def json(self):
        """Jsonify"""

        return {
            "id": self.id,
            "ts": self.ts,
            "host": self.host_id,
            "message": self.message
        }


class Filesystem(db.Model):
    """Filesystem"""
    id = id_column()
    name = db.Column(db.String(256), nullable=False)
    usage = db.relationship("Usage", backref="filesystem")
    host_id = db.Column(None, db.ForeignKey("host.id"), nullable=False)
    __table_args__ = (UniqueConstraint("host_id", "name"), )

    def json(self):
        """Jsonify"""

        return {"id": self.id, "name": self.name, "host": self.host_id}


class Usage(db.Model):
    """Quota and Usage"""
    id = id_column()
    soft = db.Column(db.BigInteger, nullable=False)
    hard = db.Column(db.BigInteger, nullable=False)
    usage = db.Column(db.BigInteger, nullable=False)
    owner_id = db.Column(None,
                         db.ForeignKey("owner.id"),
                         nullable=False,
                         index=True)
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            nullable=False,
                            index=True)
    filesystem_id = db.Column(None,
                              db.ForeignKey("filesystem.id"),
                              nullable=False,
                              index=True)

    def json(self):
        """Jsonify"""

        return {
            "owner": self.owner_id,
            "filesystem": self.filesystem_id,
            "snapshot": self.snapshot_id,
            "soft": self.soft,
            "hard": self.hard,
            "usage": self.usage
        }

# Endpoints


class SnapshotResource(QueryResource):
    """Snapshot Endpoint"""
    query_class = Snapshot


class HostResource(QueryResource):
    """Host Endpoint"""
    query_class = Host


class FilesystemResource(QueryResource):
    """Filesystem Endpoint"""
    query_class = Filesystem


class OwnerResource(QueryResource):
    """Owner Endpoint"""
    query_class = Owner


class UsageResource(QueryResource):
    """Usage Endpoint"""
    query_class = Usage

    @require_auth
    def put(self):
        """Ingest usage."""

        @lru_cache(maxsize=10000)
        def cache(model, **kwargs):
            return get_or_create(model, **kwargs)

        args = INPUT_PARSER.parse_args()
        # TODO: REGISTER INPUT HERE

        tsv = io.StringIO()

        for ingest_pass in [1, 2]:
            for message in request.json:
                data = message["data"]

                host = cache(Host, name=data["hostname"])
                snapshot = cache(Snapshot,
                                 ts=data["timestamp"],
                                 host=host,
                                 message=message["id"])

                for entry in data["filesystems"]:
                    filesystem = cache(Filesystem,
                                       name=entry["filesystem"],
                                       host=host)

                    for record in entry["quota"]:
                        owner = cache(Owner, name=record["username"])

                        if ingest_pass == 2:
                            columns = [
                                uuid.uuid4(), record["soft"], record["hard"],
                                record["used"], owner.id, snapshot.id,
                                filesystem.id
                            ]

                            tsv.write("\t".join([str(c) for c in columns]) +
                                      "\n")

            if ingest_pass == 2:
                tsv.seek(0)

                cursor = db.session.connection().connection.cursor()
                cursor.copy_from(tsv, "usage")

            commit()

        return "", 204


def setup():
    """Let's roll."""
    resources = {
        "/snapshot": SnapshotResource,
        "/host": HostResource,
        "/filesystem": FilesystemResource,
        "/owner": OwnerResource,
        "/usage": UsageResource
    }

    configure(resources)


setup()

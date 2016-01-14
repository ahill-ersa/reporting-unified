#!/usr/bin/python3
"""Application and persistence management."""

# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name

from functools import lru_cache

from ersa_reporting import db, id_column, configure
from ersa_reporting import get_or_create, commit, app, to_dict
from ersa_reporting import add, delete, request, require_auth
from ersa_reporting import Resource, QueryResource, record_input


class Owner(db.Model):
    """Storage Owner"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    usage = db.relationship("Usage", backref="owner")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class Project(db.Model):
    """Storage Group/Project"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    usage = db.relationship("Usage", backref="project")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class Host(db.Model):
    """Storage Host"""
    id = id_column()
    name = db.Column(db.String(256), unique=True, nullable=False)
    filesystems = db.relationship("Filesystem", backref="host")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class Snapshot(db.Model):
    """Storage Snapshot"""
    id = id_column()
    ts = db.Column(db.Integer, nullable=False)
    usage = db.relationship("Usage", backref="snapshot")
    bavail = db.Column(db.BigInteger)
    bfree = db.Column(db.BigInteger)
    blocks = db.Column(db.BigInteger)
    bsize = db.Column(db.Integer)
    favail = db.Column(db.BigInteger)
    ffree = db.Column(db.BigInteger)
    files = db.Column(db.BigInteger)
    frsize = db.Column(db.Integer)
    filesystem_id = db.Column(None,
                              db.ForeignKey("filesystem.id"),
                              nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self,
                       ["ts", "filesystem_id", "bavail", "bfree", "blocks",
                        "bsize", "favail", "ffree", "files", "frsize"])


class Filesystem(db.Model):
    """Filesystem"""
    id = id_column()
    name = db.Column(db.String(256), unique=True, nullable=False)
    snapshots = db.relationship("Snapshot", backref="filesystem")
    host_id = db.Column(None, db.ForeignKey("host.id"), nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "host"])


class Usage(db.Model):
    """Owner/Group Usage"""
    id = id_column()
    blocks = db.Column(db.BigInteger, nullable=False)
    bytes = db.Column(db.BigInteger, nullable=False)
    files = db.Column(db.BigInteger, nullable=False)
    owner_id = db.Column(None,
                         db.ForeignKey("owner.id"),
                         nullable=False,
                         index=True)
    project_id = db.Column(None,
                           db.ForeignKey("project.id"),
                           nullable=False,
                           index=True)
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            nullable=False,
                            index=True)

    def json(self):
        """JSON"""
        return to_dict(self, ["blocks", "bytes", "files", "owner_id",
                              "project_id", "snapshot_id"])


class OwnerResource(QueryResource):
    query_class = Owner


class ProjectResource(QueryResource):
    query_class = Project


class HostResource(QueryResource):
    query_class = Host


class SnapshotResource(QueryResource):
    query_class = Snapshot


class FilesystemResource(QueryResource):
    query_class = Filesystem


class UsageResource(QueryResource):
    query_class = Usage


class IngestResource(Resource):
    @require_auth
    def put(self):
        """Ingest usage."""

        for message in request.json:
            inserts = []

            data = message["data"]
            host = get_or_create(Host, name=data["hostname"])

            metadata = data["fs"]
            filesystem = get_or_create(Filesystem,
                                       name=metadata["name"],
                                       host=host)

            snapshot = get_or_create(Snapshot,
                                     ts=data["timestamp"],
                                     filesystem=filesystem,
                                     bavail=metadata["bavail"],
                                     bfree=metadata["bfree"],
                                     blocks=metadata["blocks"],
                                     bsize=metadata["bsize"],
                                     favail=metadata["favail"],
                                     ffree=metadata["ffree"],
                                     files=metadata["files"],
                                     frsize=metadata["frsize"])

            for who, details in data["usage"].items():
                who = who.split("/")
                owner = get_or_create(Owner, name=who[0])
                project = get_or_create(Project, name=who[1])

                add(Usage(owner=owner,
                          project=project,
                          snapshot=snapshot,
                          blocks=details["blocks"],
                          bytes=details["bytes"],
                          files=details["files"]))

        commit()

        return "", 204


def setup():
    """Let's roll."""

    resources = {
        "/snapshot": SnapshotResource,
        "/owner": OwnerResource,
        "/project": ProjectResource,
        "/host": HostResource,
        "/filesystem": FilesystemResource,
        "/usage": UsageResource,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

#!/usr/bin/python3
"""Application and persistence management."""

# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name

from functools import lru_cache

from ersa_reporting import db, id_column, configure
from ersa_reporting import get_or_create, commit, app, to_dict
from ersa_reporting import add, delete, request, require_auth
from ersa_reporting import Resource, QueryResource, record_input

# Data Models


class AvailabilityZone(db.Model):
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    volumes = db.relationship("Volume", backref="availability_zone")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class Snapshot(db.Model):
    """A snapshot of the world."""
    id = id_column()
    ts = db.Column(db.Integer, unique=True, nullable=False)
    states = db.relationship("VolumeState", backref="snapshot")
    attachments = db.relationship("VolumeAttachment", backref="snapshot")

    def json(self):
        """JSON"""
        return to_dict(self, ["ts"])


class Volume(db.Model):
    id = id_column()
    openstack_id = db.Column(db.String(64), nullable=False, unique=True)
    owner = db.Column(db.String(64), nullable=False)
    tenant = db.Column(db.String(64), nullable=False)
    availability_zone_id = db.Column(None,
                                     db.ForeignKey("availability_zone.id"))
    attachments = db.relationship("VolumeAttachment", backref="volume")
    states = db.relationship("VolumeState", backref="volume")

    def json(self):
        """JSON"""
        return to_dict(self, ["id", "openstack_id", "availability_zone_id",
                              "owner", "tenant"])


class VolumeSnapshot(db.Model):
    id = id_column()
    openstack_id = db.Column(db.String(64), nullable=False, unique=True)
    size = db.Column(db.Integer, nullable=False)
    name = db.Column(db.String(128))
    description = db.Column(db.String(512))
    source = db.Column(db.String(64), nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["id", "openstack_id", "name", "size",
                              "description", "source"])


class VolumeStatus(db.Model):
    id = id_column()
    name = db.Column(db.String(64), nullable=False, unique=True)
    states = db.relationship("VolumeState", backref="status")

    def json(self):
        """JSON"""
        return to_dict(self, ["id", "name"])


class VolumeState(db.Model):
    id = id_column()
    name = db.Column(db.String(128))
    size = db.Column(db.Integer, nullable=False, index=True)
    snapshot_id = db.Column(None, db.ForeignKey("snapshot.id"), nullable=False)
    volume_id = db.Column(None, db.ForeignKey("volume.id"), nullable=False)
    status_id = db.Column(None,
                          db.ForeignKey("volume_status.id"),
                          nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["id", "name", "size", "snapshot_id", "volume_id",
                              "status_id"])


class VolumeAttachment(db.Model):
    id = id_column()
    instance = db.Column(db.String(64), nullable=False)
    volume_id = db.Column(None, db.ForeignKey("volume.id"), primary_key=True)
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            primary_key=True)

    def json(self):
        """JSON"""
        return to_dict(self, ["id", "instance", "volume_id", "snapshot_id"])


class SnapshotResource(QueryResource):
    query_class = Snapshot


class AvailabilityZoneResource(QueryResource):
    query_class = AvailabilityZone


class VolumeResource(QueryResource):
    query_class = Volume


class VolumeSnapshotResource(QueryResource):
    query_class = VolumeSnapshot


class VolumeStatusResource(QueryResource):
    query_class = VolumeStatus


class VolumeStateResource(QueryResource):
    query_class = VolumeState


class VolumeAttachmentResource(QueryResource):
    query_class = VolumeAttachment


class IngestResource(Resource):
    @require_auth
    def put(self):
        """Data ingest"""

        @lru_cache(maxsize=100000)
        def cache(model, **kwargs):
            return get_or_create(model, **kwargs)

        for message in request.json:
            data = message["data"]

            snapshot = cache(Snapshot, ts=data["timestamp"])

            if "volumes" in data:
                for metadata in data["volumes"]:
                    az = None
                    if "availability_zone" in metadata:
                        az = cache(AvailabilityZone,
                                   name=metadata["availability_zone"])

                    volume = cache(
                        Volume,
                        openstack_id=metadata["id"],
                        availability_zone=az,
                        owner=metadata["user_id"],
                        tenant=metadata["os-vol-tenant-attr:tenant_id"])

                    status = cache(VolumeStatus, name=metadata["status"])

                    cache(VolumeState,
                          name=metadata["name"],
                          size=metadata["size"],
                          status=status,
                          snapshot=snapshot,
                          volume=volume)

                    for instance in metadata["attachments"]:
                        cache(VolumeAttachment,
                              instance=instance["server_id"],
                              volume=volume,
                              snapshot=snapshot)

            if "volume_snapshots" in data:
                for metadata in data["volume_snapshots"]:
                    cache(VolumeSnapshot,
                          openstack_id=metadata["id"],
                          name=metadata["name"],
                          description=metadata["description"],
                          size=metadata["size"],
                          source=metadata["volume_id"])

        commit()

        return "", 204


def setup():
    """Let's roll."""

    resources = {
        "/snapshot": SnapshotResource,
        "/az": AvailabilityZoneResource,
        "/volume": VolumeResource,
        "/volume/snapshot": VolumeSnapshotResource,
        "/volume/status": VolumeStatusResource,
        "/volume/state": VolumeStateResource,
        "/volume/attachment": VolumeAttachmentResource,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

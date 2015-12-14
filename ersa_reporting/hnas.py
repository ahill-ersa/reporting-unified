#!/usr/bin/python3
"""Application and persistence management."""

# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name

from functools import lru_cache

from ersa_reporting import add, app, db, configure, get_or_create
from ersa_reporting import record_input, to_dict, request, id_column
from ersa_reporting import commit, require_auth, Resource, QueryResource


def extract_allocation(name):
    if not name.islower():
        return None
    name = name.split("-")
    try:
        return int(name[-1])
    except ValueError:
        return None


class Allocation(db.Model):
    """Storage Allocation"""
    id = id_column()
    allocation = db.Column(db.Integer, unique=True, nullable=False)
    filesystems = db.relationship("Filesystem", backref="allocation")
    virtual_volumes = db.relationship("VirtualVolume", backref="allocation")

    def json(self):
        """JSON"""
        return to_dict(self, ["allocation"])


class Owner(db.Model):
    """Storage Account/Owner"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    virtual_volume_usage = db.relationship("VirtualVolumeUsage",
                                           backref="owner")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class Snapshot(db.Model):
    """Storage Snapshot"""
    id = id_column()
    ts = db.Column(db.Integer, nullable=False, unique=True)
    filesystem_usage = db.relationship("FilesystemUsage", backref="snapshot")
    virtual_volume_usage = db.relationship("VirtualVolumeUsage",
                                           backref="snapshot")

    def json(self):
        """JSON"""
        return to_dict(self, ["ts"])


class Filesystem(db.Model):
    """Filesystem"""
    id = id_column()
    name = db.Column(db.String(256), unique=True, nullable=False)
    virtual_volumes = db.relationship("VirtualVolume", backref="filesystem")
    usage = db.relationship("FilesystemUsage", backref="filesystem")
    allocation_id = db.Column(None, db.ForeignKey("allocation.id"))

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "allocation"])


class VirtualVolume(db.Model):
    """Virtual Volume"""
    id = id_column()
    name = db.Column(db.String(256), unique=True, nullable=False)
    usage = db.relationship("VirtualVolumeUsage", backref="virtual_volume")
    filesystem_id = db.Column(None,
                              db.ForeignKey("filesystem.id"),
                              index=True,
                              nullable=False)
    allocation_id = db.Column(None, db.ForeignKey("allocation.id"))

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "filesystem", "allocation"])


class FilesystemUsage(db.Model):
    """Filesystem Usage"""
    id = id_column()
    capacity = db.Column(db.BigInteger, nullable=False)
    free = db.Column(db.BigInteger, nullable=False)
    live_usage = db.Column(db.BigInteger, nullable=False)
    snapshot_usage = db.Column(db.BigInteger, nullable=False)
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            index=True,
                            nullable=False)
    filesystem_id = db.Column(None,
                              db.ForeignKey("filesystem.id"),
                              index=True,
                              nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["capacity", "free", "live_usage",
                              "snapshot_usage", "snapshot", "filesystem"])


class VirtualVolumeUsage(db.Model):
    """Virtual Volume Usage"""
    id = id_column()
    files = db.Column(db.BigInteger, nullable=False)
    quota = db.Column(db.BigInteger, nullable=False)
    usage = db.Column(db.BigInteger, nullable=False)
    owner_id = db.Column(None, db.ForeignKey("owner.id"))
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            index=True,
                            nullable=False)
    virtual_volume_id = db.Column(None,
                                  db.ForeignKey("virtual_volume.id"),
                                  index=True,
                                  nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["files", "quota", "usage", "owner", "snapshot",
                              "virtual_volume"])


class AllocationResource(QueryResource):
    query_class = Allocation


class OwnerResource(QueryResource):
    query_class = Owner


class SnapshotResource(QueryResource):
    query_class = Snapshot


class FilesystemResource(QueryResource):
    query_class = Filesystem


class VirtualVolumeResource(QueryResource):
    query_class = VirtualVolume


class FilesystemUsageResource(QueryResource):
    query_class = FilesystemUsage


class VirtualVolumeUsageResource(QueryResource):
    query_class = VirtualVolumeUsage


class IngestResource(Resource):
    @require_auth
    def put(self):
        """Ingest usage."""

        @lru_cache(maxsize=1000)
        def cache(model, **kwargs):
            return get_or_create(model, **kwargs)

        record_input()

        for message in request.json:
            if not message["schema"] == "hnas.filesystems":
                continue

            data = message["data"]

            snapshot = cache(Snapshot, ts=data["timestamp"])

            for name, details in data["filesystems"].items():
                allocation = None
                allocation_id = extract_allocation(name)
                if allocation_id:
                    allocation = cache(Allocation, allocation=allocation_id)

                fs = cache(Filesystem, name=name, allocation=allocation)
                fs_usage = {
                    "filesystem": fs,
                    "snapshot": snapshot,
                    "capacity": details["capacity"],
                    "free": details["free"],
                    "live_usage": details["live-fs-used"],
                    "snapshot_usage": details["snapshot-used"]
                }

                add(FilesystemUsage(**fs_usage))

                if "virtual_volumes" in details:
                    for vusage in details["virtual_volumes"]:
                        name = vusage["volume-name"]
                        if name.startswith("/"):
                            name = name[1:]

                        allocation = None
                        allocation_id = extract_allocation(name)
                        if allocation_id:
                            allocation = cache(Allocation,
                                               allocation=allocation_id)

                        vivol = cache(VirtualVolume,
                                      name=name,
                                      filesystem=fs,
                                      allocation=allocation)

                        vivol_usage = {
                            "snapshot": snapshot,
                            "virtual_volume": vivol,
                            "files": vusage["file-count"],
                            "usage": vusage["usage"],
                            "quota": vusage["usage-limit"]
                        }

                        if len(vusage["user-group-account"]) > 0:
                            owner = cache(Owner,
                                          name=vusage["user-group-account"])
                            vivol_usage["owner"] = owner

                        add(VirtualVolumeUsage(**vivol_usage))

        commit()

        return "", 204


def setup():
    """Let's roll."""

    resources = {
        "/allocation": AllocationResource,
        "/owner": OwnerResource,
        "/snapshot": SnapshotResource,
        "/filesystem": FilesystemResource,
        "/virtual-volume": VirtualVolumeResource,
        "/filesystem/usage": FilesystemUsageResource,
        "/virtual-volume/usage": VirtualVolumeUsageResource,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

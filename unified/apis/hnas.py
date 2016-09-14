import uuid
from functools import lru_cache

from . import app, configure, request
from . import add, get_or_create, commit
from . import QueryResource, BaseIngestResource, RangeQuery

from ..models.hnas import (
    Snapshot, Owner, Filesystem, VirtualVolume, FilesystemUsage, VirtualVolumeUsage)


class SnapshotResource(QueryResource):
    query_class = Snapshot


class OwnerResource(QueryResource):
    query_class = Owner


class FilesystemResource(QueryResource):
    query_class = Filesystem


class FilesystemSummary(RangeQuery):
    def _get(self, id='', **kwargs):
        try:
            uuid.UUID(id)
        except:
            return {}

        rslt = {}
        fs = Filesystem.query.get(id)
        if fs:
            rslt = fs.summarise(start_ts=kwargs['start'], end_ts=kwargs['end'])
        return rslt


class VirtualVolumeResource(QueryResource):
    query_class = VirtualVolume


class VirtualVolumeSummary(RangeQuery):
    def _get(self, id='', **kwargs):
        try:
            uuid.UUID(id)
        except:
            return []

        rslt = []
        vv = VirtualVolume.query.get(id)
        if vv:
            rslt = vv.summarise(start_ts=kwargs['start'], end_ts=kwargs['end'])
        return rslt


class FilesystemUsageResource(QueryResource):
    query_class = FilesystemUsage


class FilesystemUsageSummary(RangeQuery):
    def _get(self, **kwargs):
        return FilesystemUsage.summarise(start_ts=kwargs['start'], end_ts=kwargs['end'])


class VirtualVolumeUsageResource(QueryResource):
    query_class = VirtualVolumeUsage


class VirtualVolumeUsageSummary(RangeQuery):
    def _get(self, **kwargs):
        return VirtualVolumeUsage.summarise(start_ts=kwargs['start'], end_ts=kwargs['end'])


class IngestResource(BaseIngestResource):
    def ingest(self):
        """Ingest usage."""

        @lru_cache(maxsize=1000)
        def cache(model, **kwargs):
            return get_or_create(model, **kwargs)

        for message in request.get_json(force=True):
            if not message["schema"] == "hnas.filesystems":
                continue

            data = message["data"]

            snapshot = cache(Snapshot, ts=data["timestamp"])

            for name, details in data["filesystems"].items():
                fs = cache(Filesystem, name=name)
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

                        vivol = cache(VirtualVolume,
                                      name=name,
                                      filesystem=fs)

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
        "/owner": OwnerResource,
        "/snapshot": SnapshotResource,
        "/filesystem": FilesystemResource,
        "/filesystem/<id>/summary": FilesystemSummary,
        "/virtual-volume": VirtualVolumeResource,
        "/virtual-volume/<id>/summary": VirtualVolumeSummary,
        "/filesystem/usage": FilesystemUsageResource,
        "/filesystem/usage/summary": FilesystemUsageSummary,
        "/virtual-volume/usage": VirtualVolumeUsageResource,
        "/virtual-volume/usage/summary": VirtualVolumeUsageSummary,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

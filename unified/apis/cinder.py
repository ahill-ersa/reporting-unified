from functools import lru_cache

from . import app, configure, request
from . import get_or_create, commit
from . import BaseIngestResource, QueryResource

from ..models.cinder import (
    Snapshot, AvailabilityZone, Volume, VolumeSnapshot, VolumeStatus, VolumeState,
    VolumeAttachment)


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


class IngestResource(BaseIngestResource):
    def ingest(self):
        """Data ingest"""

        @lru_cache(maxsize=100000)
        def cache(model, **kwargs):
            return get_or_create(model, **kwargs)

        for message in request.get_json(force=True):
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

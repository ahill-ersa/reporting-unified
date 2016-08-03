import io
import uuid
from functools import lru_cache

from . import app, configure, request
from . import db, get_or_create, commit
from . import QueryResource, BaseIngestResource, RangeQuery

from ..models.xfs import (
    Snapshot, Host, Filesystem, Owner, Usage)


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


class IngestResource(BaseIngestResource):
    def ingest(self):
        """Ingest usage."""

        @lru_cache(maxsize=10000)
        def cache(model, **kwargs):
            return get_or_create(model, **kwargs)

        # This one is experimentally optimised for performance.
        # Internally creates a TSV and copies it straight
        # into the database.
        # Probably not necessary though.

        tsv = io.StringIO()

        for ingest_pass in [1, 2]:
            for message in request.get_json(force=True):
                if message["schema"] != "xfs.quota.report":
                    continue

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


class SnapshotSummary(RangeQuery):
    def _get(self, **kwargs):
        return Snapshot.summarise(start_ts=kwargs['start'], end_ts=kwargs['end'])


class FilesystemSummary(RangeQuery):
    def _get(self, id='', **kwargs):
        try:
            uuid.UUID(id)
        except:
            return []

        rslt = []
        fs = Filesystem.query.get(id)
        if fs:
            rslt = fs.summarise(start_ts=kwargs['start'], end_ts=kwargs['end'])
        return rslt


def setup():
    """Let's roll."""
    resources = {
        "/snapshot": SnapshotResource,
        "/snapshot/summary": SnapshotSummary,
        "/host": HostResource,
        "/filesystem": FilesystemResource,
        "/filesystem/<id>/summary": FilesystemSummary,
        "/owner": OwnerResource,
        "/usage": UsageResource,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

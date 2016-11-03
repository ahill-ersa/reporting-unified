from . import app, configure, request
from . import get_or_create, add, commit
from . import QueryResource, BaseIngestResource

from ..models.fs import (
    Owner, Project, Host, Snapshot, Filesystem, Usage
)


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


class IngestResource(BaseIngestResource):
    def ingest(self):
        """Ingest usage."""

        for message in request.get_json(force=True):
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

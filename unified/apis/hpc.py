from . import app, configure, request
from . import get_or_create, commit
from . import QueryResource, BaseIngestResource

from ..models.hpc import (
    Queue, Host, Owner, Allocation, Job
)


class QueueResource(QueryResource):
    """HPC Queue"""
    query_class = Queue


class HostResource(QueryResource):
    """HPC Host"""
    query_class = Host


class OwnerResource(QueryResource):
    """HPC Job Owner"""
    query_class = Owner


class AllocationResource(QueryResource):
    """HPC Job-Host Allocation"""
    query_class = Allocation


class JobResource(QueryResource):
    """HPC Job"""
    query_class = Job


class IngestResource(BaseIngestResource):
    def ingest(self):
        """Ingest jobs."""

        messages = [message
                    for message in request.get_json(force=True)
                    if message["data"].get("state") == "exited"]

        for message in messages:
            data = message["data"]

            queue = get_or_create(Queue, name=data["queue"])
            owner = get_or_create(Owner, name=data["owner"])

            job = get_or_create(Job, job_id=data["jobid"])
            job.name = data["jobname"]
            job.queue = queue
            job.owner = owner
            job.start = data["start"]
            job.end = data["end"]

            total_cores = 0

            for hostname, slots in data["exec_host"].items():
                host = get_or_create(Host, name=hostname)
                get_or_create(Allocation, job=job, host=host, cores=len(slots))
                total_cores += len(slots)

            job.cores = total_cores
            job.cpu_seconds = total_cores * (data["end"] - data["start"])

        commit()

        return "", 204


def setup():
    """Let's roll."""

    resources = {
        "/host": HostResource,
        "/queue": QueueResource,
        "/owner": OwnerResource,
        "/job": JobResource,
        "/allocation": AllocationResource,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

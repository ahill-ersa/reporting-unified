import uuid
from . import app, configure, request
from . import get_or_create, commit
from . import QueryResource, BaseIngestResource, RangeQuery

from ..models.hpc import Queue, Host, Owner, Allocation, Job


class QueueResource(QueryResource):
    """HPC Queue"""
    query_class = Queue


class HostResource(QueryResource):
    """HPC Host"""
    query_class = Host


class HostSummary(RangeQuery):
    def _get(self, id='', **kwargs):
        try:
            uuid.UUID(id)
        except:
            return {}

        rslt = {}
        host = Host.query.get(id)
        if host:
            rslt = host.summarise(start_ts=kwargs['start'], end_ts=kwargs['end'])
        return rslt


class OwnerResource(QueryResource):
    """HPC Job Owner"""
    query_class = Owner


class OwnerSummary(RangeQuery):
    def _get(self, id='', **kwargs):
        try:
            uuid.UUID(id)
        except:
            return []

        rslt = []
        owner = Owner.query.get(id)
        if owner:
            rslt = owner.summarise(start_ts=kwargs['start'], end_ts=kwargs['end'])
        return rslt


class AllocationResource(QueryResource):
    """HPC Job-Host Allocation"""
    query_class = Allocation


class AllocationSummary(RangeQuery):
    def _get(self, **kwargs):
        return Allocation.summarise(start_ts=kwargs['start'], end_ts=kwargs['end'])


class AllocationRuntimeSummary(RangeQuery):
    """Gets job run statistics finished between start_ts and end_ts.

    Similar to AllocationSummary but includes run time. Grouped by host
    """
    def _get(self, **kwargs):
        return Allocation.summarise_runtime(start_ts=kwargs['start'], end_ts=kwargs['end'])


class JobResource(QueryResource):
    """HPC Job"""
    query_class = Job


class JobList(RangeQuery):
    def _get(self, **kwargs):
        return Job.list(start_ts=kwargs['start'], end_ts=kwargs['end'])


class JobSummary(RangeQuery):
    def _get(self, **kwargs):
        return Job.summarise(start_ts=kwargs['start'], end_ts=kwargs['end'])


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
        "/host/<id>/summary": HostSummary,
        "/queue": QueueResource,
        "/owner": OwnerResource,
        "/owner/<id>/summary": OwnerSummary,
        "/job": JobResource,
        "/job/list": JobList,
        "/job/summary": JobSummary,
        "/allocation": AllocationResource,
        "/allocation/summary": AllocationSummary,
        "/allocation/runtime/summary": AllocationRuntimeSummary,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

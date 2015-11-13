#!/usr/bin/python3
"""Application and persistence management."""

# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name

from ersa_reporting import db, id_column, configure, get_or_create, commit
from ersa_reporting import app, request, require_auth, QueryResource

# Data Models


class Owner(db.Model):
    """HPC Job Owner"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    jobs = db.relationship("Job", backref="owner")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class Host(db.Model):
    """HPC Host"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    allocations = db.relationship("Allocation", backref="host")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class Queue(db.Model):
    """HPC Queue"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    jobs = db.relationship("Job", backref="queue")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class Job(db.Model):
    """HPC Job"""
    id = id_column()
    job_id = db.Column(db.String(64), unique=True, nullable=False)
    owner_id = db.Column(None, db.ForeignKey("owner.id"))
    queue_id = db.Column(None, db.ForeignKey("queue.id"))
    name = db.Column(db.String(64))
    start = db.Column(db.Integer)
    end = db.Column(db.Integer)
    cores = db.Column(db.Integer)
    cpu_seconds = db.Column(db.Integer)
    allocations = db.relationship("Allocation", backref="job")

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "job_id": self.job_id,
            "name": self.name,
            "owner": self.owner_id,
            "queue": self.queue_id,
            "start": self.start,
            "end": self.end,
            "cores": self.cores,
            "cpu_seconds": self.cpu_seconds
        }


class Allocation(db.Model):
    """HPC Job-Host Mapping"""
    id = id_column()
    job_id = db.Column(None, db.ForeignKey("job.id"))
    host_id = db.Column(None, db.ForeignKey("host.id"))
    cores = db.Column(db.Integer)

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "job": self.job_id,
            "host": self.host_id,
            "cores": self.cores
        }

# Endpoints


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

    @require_auth
    def put(self):
        """Ingest jobs."""
        messages = [message for message in request.json
                    if message["data"]["state"] == "exited"]

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
                get_or_create(Allocation,
                              job=job,
                              host=host,
                              cores=len(slots))
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
        "/allocation": AllocationResource
    }

    configure(resources)


setup()

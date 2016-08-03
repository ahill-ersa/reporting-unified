from . import db, id_column


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
    name = db.Column(db.String(256))
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

    @classmethod
    def list(cls, start_ts=0, end_ts=0):
        """"Gets jobs finished between start_ts and end_ts.
        """
        query = cls.query.join(Owner).join(Queue).\
            with_entities(Queue.name, Owner.name, Job.job_id, Job.cores, Job.cpu_seconds)

        if start_ts > 0:
            query = query.filter(Job.end >= start_ts)
        if end_ts > 0:
            query = query.filter(Job.end < end_ts)
        fields = ['queue', 'owner', 'job_id', 'cores', 'cpu_seconds']
        return [dict(zip(fields, q)) for q in query.all()]


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

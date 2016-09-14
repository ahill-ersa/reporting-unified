from sqlalchemy.sql import func
from . import db, id_column


class Owner(db.Model):
    """HPC Job Owner"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    jobs = db.relationship("Job", backref="owner")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}

    def summarise(self, start_ts=0, end_ts=0):
        """"Gets job statistics of which finished between start_ts and end_ts.

        Grouped by queue
        """
        id_query = Job.id_between(start_ts, end_ts)

        query = self.query.join(Job).join(Queue).\
            filter(Job.id.in_(id_query)).\
            filter(Job.owner_id == self.id).\
            group_by(Queue.name).\
            with_entities(Queue.name,
                          func.count(Job.job_id),
                          func.sum(Job.cores),
                          func.sum(Job.cpu_seconds))

        fields = ['queue', 'job_count', 'cores', 'cpu_seconds']
        return [dict(zip(fields, q)) for q in query.all()]


class Host(db.Model):
    """HPC Host"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    allocations = db.relationship("Allocation", backref="host")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}

    def summarise(self, start_ts=0, end_ts=0):
        """"Gets job statistics of which finished between start_ts and end_ts.
        """
        id_query = Job.id_between(start_ts, end_ts)

        allocated_jobs = Allocation.query.\
            filter(Allocation.host_id == self.id).\
            with_entities(Allocation.job_id).subquery()

        query = Job.query.filter(Job.id.in_(id_query)).\
            filter(Job.id.in_(allocated_jobs)).\
            with_entities(func.count(Job.job_id),
                          func.sum(Job.cores),
                          func.sum(Job.cpu_seconds))

        values = query.first()
        if values[0]:
            return dict(zip(['job_count', 'cores', 'cpu_seconds'], values))
        else:
            return {}


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
    def id_between(cls, start_ts=0, end_ts=0):
        """"Gets snapshot ids between start_ts and end_ts.

        It returns a subquery not actual values.
        """
        id_query = cls.query
        if start_ts > 0:
            id_query = id_query.filter(Job.end >= start_ts)
        if end_ts > 0:
            id_query = id_query.filter(Job.end < end_ts)
        return id_query.with_entities(cls.id).subquery()

    @classmethod
    def list(cls, start_ts=0, end_ts=0):
        """"Gets jobs finished between start_ts and end_ts.
        """
        query = cls.query.join(Owner).join(Queue).\
            with_entities(Job.job_id, Job.name,
                          Queue.name, Owner.name,
                          Job.start, Job.end,
                          Job.cores, Job.cpu_seconds)

        if start_ts > 0:
            query = query.filter(Job.end >= start_ts)
        if end_ts > 0:
            query = query.filter(Job.end < end_ts)
        fields = ['job_id', 'name', 'queue', 'owner', 'start',
                  'end', 'cores', 'cpu_seconds']
        return [dict(zip(fields, q)) for q in query.all()]

    @classmethod
    def summarise(cls, start_ts=0, end_ts=0):
        """"Gets job statistics of which finished between start_ts and end_ts.

        Grouped by owner then queue
        """
        id_query = cls.id_between(start_ts, end_ts)

        query = cls.query.join(Owner).join(Queue).\
            filter(cls.id.in_(id_query)).\
            group_by(Owner.name, Queue.name).\
            with_entities(Owner.name, Queue.name,
                          func.count(Job.job_id),
                          func.sum(Job.cores),
                          func.sum(Job.cpu_seconds))

        fields = ['owner', 'queue', 'job_count', 'cores', 'cpu_seconds']
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

    @classmethod
    def summarise(cls, start_ts=0, end_ts=0):
        """"Gets job statistics finished between start_ts and end_ts.

        Grouped by host
        """
        id_query = Job.id_between(start_ts, end_ts)

        query = cls.query.join(Host).\
            filter(cls.job_id.in_(id_query)).\
            group_by(Host.name).\
            with_entities(Host.name,
                          func.count(cls.job_id),
                          func.sum(cls.cores))

        fields = ['host', 'job_count', 'cores']
        return [dict(zip(fields, q)) for q in query.all()]

    @classmethod
    def summarise_runtime(cls, start_ts=0, end_ts=0):
        """"Gets job run statistics finished between start_ts and end_ts.

        Similar to summarise but includes run time. Grouped by host
        """
        id_query = Job.id_between(start_ts, end_ts)

        query = cls.query.join(Host).join(Job).\
            filter(cls.job_id.in_(id_query)).\
            group_by(Host.name).\
            with_entities(Host.name,
                          func.count(cls.job_id),
                          func.sum(cls.cores),
                          func.sum(Job.cpu_seconds))

        fields = ['host', 'job_count', 'cores', 'cpu_seconds']
        return [dict(zip(fields, q)) for q in query.all()]

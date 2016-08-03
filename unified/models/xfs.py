from sqlalchemy.sql import func
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from . import db, id_column


class Owner(db.Model):
    """Storage Owner"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    usage = db.relationship("Usage", backref="owner")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class Host(db.Model):
    """Storage Host"""
    id = id_column()
    name = db.Column(db.String(256), unique=True, nullable=False)
    snapshots = db.relationship("Snapshot", backref="host")
    filesystems = db.relationship("Filesystem", backref="host")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class Filesystem(db.Model):
    """Filesystem"""
    id = id_column()
    name = db.Column(db.String(256), nullable=False)
    usage = db.relationship("Usage", backref="filesystem")
    host_id = db.Column(None, db.ForeignKey("host.id"), nullable=False)
    __table_args__ = (UniqueConstraint("host_id", "name"), )

    def json(self):
        """Jsonify"""

        return {"id": self.id, "name": self.name, "host": self.host_id}

    def summarise(self, start_ts=0, end_ts=0):
        """"Gets usage of a file system between start_ts and end_ts.

        Maximal usage of the period is returned.
        """
        snapshot_ids = Snapshot.id_between(start_ts, end_ts)
        query = Usage.query.join(Owner).\
            filter(Usage.filesystem_id == self.id).\
            filter(Usage.snapshot_id.in_(snapshot_ids)).\
            group_by(Owner.name).\
            with_entities(Owner.name,
                          func.max(Usage.soft).label('soft'),
                          func.max(Usage.hard).label('hard'),
                          func.max(Usage.usage).label('usage'))
        fields = ['owner', 'soft', 'hard', 'usage']
        return [dict(zip(fields, q)) for q in query.all()]


class Usage(db.Model):
    """Quota and Usage"""
    id = id_column()
    soft = db.Column(db.BigInteger, nullable=False)
    hard = db.Column(db.BigInteger, nullable=False)
    usage = db.Column(db.BigInteger, nullable=False)
    owner_id = db.Column(None,
                         db.ForeignKey("owner.id"),
                         nullable=False,
                         index=True)
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            nullable=False,
                            index=True)
    filesystem_id = db.Column(None,
                              db.ForeignKey("filesystem.id"),
                              nullable=False,
                              index=True)

    def json(self):
        """Jsonify"""

        return {
            "owner": self.owner_id,
            "filesystem": self.filesystem_id,
            "snapshot": self.snapshot_id,
            "soft": self.soft,
            "hard": self.hard,
            "usage": self.usage
        }


class Snapshot(db.Model):
    """Storage Snapshot"""
    id = id_column()
    ts = db.Column(db.Integer, nullable=False)
    usage = db.relationship("Usage", backref="snapshot")
    host_id = db.Column(None, db.ForeignKey("host.id"), nullable=False)
    message = db.Column(UUID, nullable=False, unique=True)

    def json(self):
        """Jsonify"""

        return {
            "id": self.id,
            "ts": self.ts,
            "host": self.host_id,
            "message": self.message
        }

    @classmethod
    def id_between(cls, start_ts=0, end_ts=0):
        """"Gets snapshop ids between start_ts and end_ts.

        It returns a subquery not actual values.
        """
        id_query = cls.query
        if start_ts > 0:
            id_query = id_query.filter(Snapshot.ts >= start_ts)
        if end_ts > 0:
            id_query = id_query.filter(Snapshot.ts < end_ts)
        return id_query.with_entities(Snapshot.id).subquery()

    @classmethod
    def summarise(cls, start_ts=0, end_ts=0):
        """"Gets usage from their snapshots between start_ts and end_ts.

        Maximal usage of the period is returned.
        """
        id_query = cls.id_between(start_ts, end_ts)

        # soft and hard quotas are not changed very often
        # Record number of Host, Filesystem and Owner are relatively very small.
        # Link them in code to avoid expand usage rows whose number is very very high
        query = Usage.query.filter(Usage.snapshot_id.in_(id_query)).\
            group_by(Usage.filesystem_id, Usage.owner_id).\
            with_entities(Usage.filesystem_id, Usage.owner_id,
                          func.max(Usage.soft).label('soft'),
                          func.max(Usage.hard).label('hard'),
                          func.max(Usage.usage).label('usage'))

        fq = Filesystem.query.join(Host).\
            with_entities(Filesystem.id, Host.name, Filesystem.name).all()
        file_systems = {}
        for fs in fq:
            file_systems[fs[0]] = fs[1:]

        owners = dict(Owner.query.with_entities(Owner.id, Owner.name).all())

        fields = ['host', 'filesystem', 'owner', 'soft', 'hard', 'usage']
        rslt = []

        for q in query.all():
            hn, fn = file_systems[q[0]]
            mappings = (hn, fn, owners[q[1]], q[2], q[3], q[4])
            rslt.append(dict(zip(fields, mappings)))
        return rslt

from datetime import timedelta
from sqlalchemy.sql import func
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from . import db, id_column, SnapshotMothods

# Days between start_ts and end_ts to switch from filtering owner locally
# to remotely in Owner.summarise. This is very ad-hoc and tested with
# usage table of 28,054,534 rows
CUT_OFF = 3


class Owner(db.Model):
    """Storage Owner"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    usage = db.relationship("Usage", backref="owner")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}

    @classmethod
    def _get_file_systems(cls):
        """Get a dict with filesystem id as keys and host and filesystem names as values"""
        if not hasattr(cls, '_fs_dict'):
            fq = Filesystem.query.join(Host).\
                with_entities(Filesystem.id, Host.name, Filesystem.name).all()
            cls._fs_dict = {}
            for fs in fq:
                cls._fs_dict[fs[0]] = fs[1:]

        return cls._fs_dict

    def _remote_filter(self, id_query):
        # Do remote owner filter because planer can efficiently find
        # relevant usage records
        query = Usage.query.filter(Usage.snapshot_id.in_(id_query)).\
            filter(Usage.owner_id == self.id).\
            group_by(Usage.filesystem_id).\
            with_entities(Usage.filesystem_id,
                          func.max(Usage.soft).label('soft'),
                          func.max(Usage.hard).label('hard'),
                          func.max(Usage.usage).label('usage'))

        fields = ['host', 'filesystem', 'soft', 'hard', 'usage']
        file_systems = self._get_file_systems()
        rslt = []

        for q in query.all():
            hn, fn = file_systems[q[0]]
            mappings = (hn, fn, q[1], q[2], q[3])
            rslt.append(dict(zip(fields, mappings)))
        return rslt

    def _local_filter(self, id_query):
        # Do local owner filter because planer chooses to use bitmapAnd when
        # fewer snapshots involved which is slow
        query = Usage.query.filter(Usage.snapshot_id.in_(id_query)).\
            group_by(Usage.owner_id, Usage.filesystem_id).\
            with_entities(Usage.owner_id, Usage.filesystem_id,
                          func.max(Usage.soft).label('soft'),
                          func.max(Usage.hard).label('hard'),
                          func.max(Usage.usage).label('usage'))

        fields = ['host', 'filesystem', 'soft', 'hard', 'usage']
        file_systems = self._get_file_systems()
        rslt = []

        for q in query.all():
            if q[0] == self.id:
                hn, fn = file_systems[q[1]]
                mappings = (hn, fn, q[2], q[3], q[4])
                rslt.append(dict(zip(fields, mappings)))
                break
        return rslt

    def summarise(self, start_ts=0, end_ts=0):
        """"Gets usage of an owner between start_ts and end_ts.

        Maximal usage of the period is returned. Grouped by filesystem
        """
        id_query = Snapshot.id_between(start_ts, end_ts)
        date_window = timedelta(seconds=(end_ts - start_ts))

        if date_window.total_seconds() == 0 or abs(date_window.days) >= CUT_OFF:
            return self._remote_filter(id_query)
        else:
            return self._local_filter(id_query)

    def list(self, start_ts=0, end_ts=0):
        """"Gets a list of usages between start_ts and end_ts.
        """
        snapshots = Snapshot.between(start_ts, end_ts)
        query = Usage.query.join(snapshots).\
            filter(Usage.owner_id == self.id).\
            order_by(Usage.filesystem_id, snapshots.c.ts).\
            with_entities(Usage.filesystem_id,
                          snapshots.c.ts,
                          Usage.soft,
                          Usage.hard,
                          Usage.usage)

        fields = ['ts', 'host', 'soft', 'hard', 'usage']
        file_systems = self._get_file_systems()
        rslt = {}

        for q in query.all():
            hn, fn = file_systems[q[0]]
            mappings = (q[1], hn, q[2], q[3], q[4])
            if fn not in rslt:
                rslt[fn] = []
            rslt[fn].append(dict(zip(fields, mappings)))
        return rslt


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

    def list(self, start_ts=0, end_ts=0):
        """"Gets usages between start_ts and end_ts.
        """
        snapshots = Snapshot.between(start_ts, end_ts)
        query = Usage.query.join(snapshots).\
            filter(Usage.filesystem_id == self.id).\
            join(Owner).\
            order_by(snapshots.c.ts).\
            with_entities(snapshots.c.ts,
                          Owner.name,
                          Usage.soft,
                          Usage.hard,
                          Usage.usage)

        fields = ['ts', 'owner', 'soft', 'hard', 'usage']
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

    @classmethod
    def summarise(cls, start_ts=0, end_ts=0):
        """"Gets usage from their snapshots between start_ts and end_ts.

        Maximal usage of the period is returned.
        """
        id_query = Snapshot.id_between(start_ts, end_ts)

        # 1. soft and hard quotas are not changed very often
        # 2. Record number of Host, Filesystem and Owner are relatively very small,
        # link them in code to avoid expand usage rows whose number is very very high
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


class Snapshot(db.Model, SnapshotMothods):
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

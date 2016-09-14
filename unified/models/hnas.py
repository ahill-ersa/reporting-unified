from sqlalchemy.sql import func
from . import db, id_column, to_dict, SnapshotMothods


class Owner(db.Model):
    """Storage Account/Owner"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    virtual_volume_usage = db.relationship("VirtualVolumeUsage",
                                           backref="owner")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class Snapshot(db.Model, SnapshotMothods):
    """Storage Snapshot"""
    id = id_column()
    ts = db.Column(db.Integer, nullable=False, unique=True)
    filesystem_usage = db.relationship("FilesystemUsage", backref="snapshot")
    virtual_volume_usage = db.relationship("VirtualVolumeUsage",
                                           backref="snapshot")

    def json(self):
        """JSON"""
        return to_dict(self, ["ts"])


class Filesystem(db.Model):
    """Filesystem"""
    id = id_column()
    name = db.Column(db.String(256), unique=True, nullable=False)
    virtual_volumes = db.relationship("VirtualVolume", backref="filesystem")
    usage = db.relationship("FilesystemUsage", backref="filesystem")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])

    def summarise(self, start_ts=0, end_ts=0):
        """"Gets usage of a file system between start_ts and end_ts.

        Maximal usage of the period is returned.
        """
        snapshot_ids = Snapshot.id_between(start_ts, end_ts)
        id_query = FilesystemUsage.query.\
            filter(FilesystemUsage.filesystem_id == self.id).\
            filter(FilesystemUsage.snapshot_id.in_(snapshot_ids)).\
            with_entities(FilesystemUsage.id)

        query = FilesystemUsage.query.filter(FilesystemUsage.id.in_(id_query)).\
            with_entities(func.max(FilesystemUsage.capacity),
                          func.min(FilesystemUsage.free),
                          func.max(FilesystemUsage.live_usage),
                          func.max(FilesystemUsage.snapshot_usage))

        values = query.first()
        if values.count(None) == len(values):
            return {}
        else:
            fields = ['capacity', 'free', 'live_usage', 'snapshot_usage']
            return dict(zip(fields, values))


class VirtualVolume(db.Model):
    """Virtual Volume"""
    id = id_column()
    name = db.Column(db.String(256), unique=True, nullable=False)
    usage = db.relationship("VirtualVolumeUsage", backref="virtual_volume")
    filesystem_id = db.Column(None,
                              db.ForeignKey("filesystem.id"),
                              index=True,
                              nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "filesystem_id"])

    def summarise(self, start_ts=0, end_ts=0):
        """"Gets usage of a virtual volume between start_ts and end_ts.

        Maximal usage of the period is returned.
        """
        snapshot_ids = Snapshot.id_between(start_ts, end_ts)
        id_query = VirtualVolumeUsage.query.\
            filter(VirtualVolumeUsage.virtual_volume_id == self.id).\
            filter(VirtualVolumeUsage.snapshot_id.in_(snapshot_ids)).\
            with_entities(VirtualVolumeUsage.id)

        query = VirtualVolumeUsage.query.\
            filter(VirtualVolumeUsage.id.in_(id_query)).\
            group_by(VirtualVolumeUsage.owner_id).\
            with_entities(VirtualVolumeUsage.owner_id,
                          func.max(VirtualVolumeUsage.quota),
                          func.max(VirtualVolumeUsage.files),
                          func.max(VirtualVolumeUsage.usage))

        fields = ['owner', 'quota', 'files', 'usage']
        rslt = []

        for q in query.all():
            values = list(q)
            # almost all usages has no owner, query owner directly if needed
            if values[0]:
                values[0] = Owner.query.get(q[0]).name
            rslt.append(dict(zip(fields, values)))
        return rslt


class FilesystemUsage(db.Model):
    """Filesystem Usage"""
    id = id_column()
    capacity = db.Column(db.BigInteger, nullable=False)
    free = db.Column(db.BigInteger, nullable=False)
    live_usage = db.Column(db.BigInteger, nullable=False)
    snapshot_usage = db.Column(db.BigInteger, nullable=False)
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            index=True,
                            nullable=False)
    filesystem_id = db.Column(None,
                              db.ForeignKey("filesystem.id"),
                              index=True,
                              nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["capacity", "free", "live_usage",
                              "snapshot_usage", "snapshot_id", "filesystem_id"])

    @classmethod
    def summarise(cls, start_ts=0, end_ts=0):
        """"Gets usage from their snapshots between start_ts and end_ts.

        Maximal usage of the period is returned.
        """
        id_query = Snapshot.id_between(start_ts, end_ts)

        query = cls.query.filter(cls.snapshot_id.in_(id_query)).\
            group_by(cls.filesystem_id).\
            with_entities(cls.filesystem_id,
                          func.max(cls.capacity).label('capacity'),
                          func.min(cls.free).label('free'),
                          func.max(cls.live_usage).label('live_usage'),
                          func.max(cls.snapshot_usage).label('snapshot_usage'))

        file_systems = dict(Filesystem.query.with_entities(Filesystem.id, Filesystem.name).all())

        fields = ['filesystem', 'capacity', 'free', 'live_usage', 'snapshot_usage']
        rslt = []

        for q in query.all():
            mappings = (file_systems[q[0]], q[1], q[2], q[3], q[4])
            rslt.append(dict(zip(fields, mappings)))
        return rslt


class VirtualVolumeUsage(db.Model):
    """Virtual Volume Usage"""
    id = id_column()
    files = db.Column(db.BigInteger, nullable=False)
    quota = db.Column(db.BigInteger, nullable=False)
    usage = db.Column(db.BigInteger, nullable=False)
    owner_id = db.Column(None, db.ForeignKey("owner.id"))
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            index=True,
                            nullable=False)
    virtual_volume_id = db.Column(None,
                                  db.ForeignKey("virtual_volume.id"),
                                  index=True,
                                  nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["files", "quota", "usage", "owner_id", "snapshot_id",
                              "virtual_volume_id"])

    @classmethod
    def summarise(cls, start_ts=0, end_ts=0):
        """"Gets usage from their snapshots between start_ts and end_ts.

        Maximal usage of the period is returned.
        """
        id_query = Snapshot.id_between(start_ts, end_ts)

        query = cls.query.filter(cls.snapshot_id.in_(id_query)).\
            group_by(cls.virtual_volume_id, cls.owner_id).\
            with_entities(cls.virtual_volume_id, cls.owner_id,
                          func.max(cls.quota).label('quota'),
                          func.max(cls.files).label('files'),
                          func.max(cls.usage).label('usage'))

        fq = VirtualVolume.query.join(Filesystem).\
            with_entities(VirtualVolume.id, Filesystem.name, VirtualVolume.name).all()
        file_systems = {}
        for fs in fq:
            file_systems[fs[0]] = fs[1:]

        # Not all virtual volumes has owner
        owners = dict(Owner.query.with_entities(Owner.id, Owner.name).all())

        fields = ['filesystem', 'virtual_volume', 'owner', 'quota', 'files', 'usage']
        rslt = []

        for q in query.all():
            fn, vn = file_systems[q[0]]
            owner = owners[q[1]] if q[1] else ''
            mappings = (fn, vn, owner, q[2], q[3], q[4])
            rslt.append(dict(zip(fields, mappings)))
        return rslt

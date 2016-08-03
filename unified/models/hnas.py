from . import db, id_column, to_dict


class Owner(db.Model):
    """Storage Account/Owner"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    virtual_volume_usage = db.relationship("VirtualVolumeUsage",
                                           backref="owner")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class Snapshot(db.Model):
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

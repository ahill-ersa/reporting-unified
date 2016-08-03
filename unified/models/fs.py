from . import db, id_column, to_dict


class Owner(db.Model):
    """Storage Owner"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    usage = db.relationship("Usage", backref="owner")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class Project(db.Model):
    """Storage Group/Project"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    usage = db.relationship("Usage", backref="project")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class Host(db.Model):
    """Storage Host"""
    id = id_column()
    name = db.Column(db.String(256), unique=True, nullable=False)
    filesystems = db.relationship("Filesystem", backref="host")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class Snapshot(db.Model):
    """Storage Snapshot"""
    id = id_column()
    ts = db.Column(db.Integer, nullable=False)
    usage = db.relationship("Usage", backref="snapshot")
    bavail = db.Column(db.BigInteger)
    bfree = db.Column(db.BigInteger)
    blocks = db.Column(db.BigInteger)
    bsize = db.Column(db.Integer)
    favail = db.Column(db.BigInteger)
    ffree = db.Column(db.BigInteger)
    files = db.Column(db.BigInteger)
    frsize = db.Column(db.Integer)
    filesystem_id = db.Column(None,
                              db.ForeignKey("filesystem.id"),
                              nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self,
                       ["ts", "filesystem_id", "bavail", "bfree", "blocks",
                        "bsize", "favail", "ffree", "files", "frsize"])


class Filesystem(db.Model):
    """Filesystem"""
    id = id_column()
    name = db.Column(db.String(256), unique=True, nullable=False)
    snapshots = db.relationship("Snapshot", backref="filesystem")
    host_id = db.Column(None, db.ForeignKey("host.id"), nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "host_id"])


class Usage(db.Model):
    """Owner/Group Usage"""
    id = id_column()
    blocks = db.Column(db.BigInteger, nullable=False)
    bytes = db.Column(db.BigInteger, nullable=False)
    files = db.Column(db.BigInteger, nullable=False)
    owner_id = db.Column(None,
                         db.ForeignKey("owner.id"),
                         nullable=False,
                         index=True)
    project_id = db.Column(None,
                           db.ForeignKey("project.id"),
                           nullable=False,
                           index=True)
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            nullable=False,
                            index=True)

    def json(self):
        """JSON"""
        return to_dict(self, ["blocks", "bytes", "files", "owner_id",
                              "project_id", "snapshot_id"])

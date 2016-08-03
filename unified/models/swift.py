from . import db, id_column, to_dict


class Account(db.Model):
    id = id_column()
    openstack_id = db.Column(db.String(64), unique=True, nullable=False)
    account_snapshots = db.relationship("Usage", backref="account")

    def json(self):
        """JSON"""
        return to_dict(self, ["openstack_id"])


class Snapshot(db.Model):
    id = id_column()
    ts = db.Column(db.Integer, unique=True, nullable=False)
    account_snapshots = db.relationship("Usage", backref="snapshot")

    def json(self):
        """JSON"""
        return to_dict(self, ["ts"])


class Usage(db.Model):
    id = id_column()
    bytes = db.Column(db.BigInteger, nullable=False)
    containers = db.Column(db.Integer, nullable=False)
    objects = db.Column(db.Integer, nullable=False)
    quota = db.Column(db.BigInteger)
    account_id = db.Column(None, db.ForeignKey("account.id"), nullable=False)
    snapshot_id = db.Column(None, db.ForeignKey("snapshot.id"), nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["bytes", "containers", "objects", "quota",
                              "account_id", "snapshot_id"])

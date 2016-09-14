from sqlalchemy.sql import func
from . import db, id_column, to_dict, SnapshotMothods


class Account(db.Model):
    id = id_column()
    openstack_id = db.Column(db.String(64), unique=True, nullable=False)
    account_snapshots = db.relationship("Usage", backref="account")

    def json(self):
        """JSON"""
        return to_dict(self, ["openstack_id"])


class Snapshot(db.Model, SnapshotMothods):
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

    @classmethod
    def summarise(cls, start_ts=0, end_ts=0):
        """"Gets usage from their snapshots between start_ts and end_ts.

        Maximal usage of the period is returned.
        """
        id_query = Snapshot.id_between(start_ts, end_ts)

        query = cls.query.filter(cls.snapshot_id.in_(id_query)).\
            group_by(cls.account_id).\
            with_entities(cls.account_id,
                          func.max(cls.quota),
                          func.max(cls.bytes),
                          func.max(cls.containers),
                          func.max(cls.objects))

        accounts = dict(Account.query.with_entities(Account.id, Account.openstack_id).all())

        fields = ['openstack_id', 'quota', 'bytes', 'containers', 'objects']
        rslt = []

        for q in query.all():
            rslt.append(dict(zip(fields, (accounts[q[0]], q[1], q[2], q[3], q[4]))))
        return rslt

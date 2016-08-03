from . import db, id_column, to_dict


class Allocation(db.Model):
    """Storage Allocation"""
    id = id_column()
    allocation = db.Column(db.Integer, unique=True, nullable=False)
    tenants = db.relationship("Tenant", backref="allocation")
    namespaces = db.relationship("Namespace", backref="allocation")

    def json(self):
        """JSON"""
        return to_dict(self, ["allocation"])


class Snapshot(db.Model):
    """Storage Snapshot"""
    id = id_column()
    ts = db.Column(db.Integer, nullable=False)
    usage = db.relationship("Usage", backref="snapshot")

    def json(self):
        """JSON"""
        return to_dict(self, ["ts"])


class Tenant(db.Model):
    """HCP Tenant"""
    id = id_column()
    name = db.Column(db.String(256), unique=True, nullable=False)
    namespaces = db.relationship("Namespace", backref="tenant")
    allocation_id = db.Column(None, db.ForeignKey("allocation.id"))

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "allocation_id"])


class Namespace(db.Model):
    """HCP Namespace"""
    id = id_column()
    name = db.Column(db.String(256), nullable=False)
    usage = db.relationship("Usage", backref="namespace")
    tenant_id = db.Column(None,
                          db.ForeignKey("tenant.id"),
                          index=True,
                          nullable=False)
    allocation_id = db.Column(None, db.ForeignKey("allocation.id"))

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "tenant_id", "allocation_id"])


class Usage(db.Model):
    """HCP Usage"""
    id = id_column()
    start_time = db.Column(db.Integer, index=True, nullable=False)
    end_time = db.Column(db.Integer, index=True, nullable=False)
    ingested_bytes = db.Column(db.BigInteger, nullable=False)
    raw_bytes = db.Column(db.BigInteger, nullable=False)
    reads = db.Column(db.BigInteger, nullable=False)
    writes = db.Column(db.BigInteger, nullable=False)
    deletes = db.Column(db.BigInteger, nullable=False)
    objects = db.Column(db.BigInteger, nullable=False)
    bytes_in = db.Column(db.BigInteger, nullable=False)
    bytes_out = db.Column(db.BigInteger, nullable=False)
    # FIXME: below four should be optional as the current producer seems not having them
    # The HCP official doc checked on 2016/05/10 at:
    # http://hcpsdk.readthedocs.io/en/latest/40_mapi/40-3_mapi-chargeback.html
    metadata_only_objects = db.Column(db.BigInteger, nullable=False)
    metadata_only_bytes = db.Column(db.BigInteger, nullable=False)
    tiered_objects = db.Column(db.BigInteger, nullable=False)
    tiered_bytes = db.Column(db.BigInteger, nullable=False)
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            index=True,
                            nullable=False)
    namespace_id = db.Column(None,
                             db.ForeignKey("namespace.id"),
                             index=True,
                             nullable=False)

    def json(self):
        """JSON"""
        return to_dict(
            self,
            ["start_time", "end_time", "ingested_bytes", "raw_bytes", "reads",
             "writes", "deletes", "objects", "bytes_in", "bytes_out",
             "metadata_only_objects", "metadata_only_bytes", "tiered_objects",
             "tiered_bytes", "snapshot_id", "namespace_id"])

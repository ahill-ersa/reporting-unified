from . import db, id_column, to_dict


class AvailabilityZone(db.Model):
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    volumes = db.relationship("Volume", backref="availability_zone")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class Snapshot(db.Model):
    """A snapshot of the world."""
    id = id_column()
    ts = db.Column(db.Integer, unique=True, nullable=False)
    states = db.relationship("VolumeState", backref="snapshot")
    attachments = db.relationship("VolumeAttachment", backref="snapshot")

    def json(self):
        """JSON"""
        return to_dict(self, ["ts"])


class Volume(db.Model):
    id = id_column()
    openstack_id = db.Column(db.String(64), nullable=False, unique=True)
    owner = db.Column(db.String(64), nullable=False)
    tenant = db.Column(db.String(64), nullable=False)
    availability_zone_id = db.Column(None,
                                     db.ForeignKey("availability_zone.id"))
    attachments = db.relationship("VolumeAttachment", backref="volume")
    states = db.relationship("VolumeState", backref="volume")

    def json(self):
        """JSON"""
        return to_dict(self, ["openstack_id", "availability_zone_id", "owner",
                              "tenant"])


class VolumeSnapshot(db.Model):
    id = id_column()
    openstack_id = db.Column(db.String(64), nullable=False, unique=True)
    size = db.Column(db.Integer, nullable=False)
    name = db.Column(db.String(128))
    description = db.Column(db.String(512))
    source = db.Column(db.String(64), nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["openstack_id", "name", "size", "description",
                              "source"])


class VolumeStatus(db.Model):
    id = id_column()
    name = db.Column(db.String(64), nullable=False, unique=True)
    states = db.relationship("VolumeState", backref="status")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class VolumeState(db.Model):
    id = id_column()
    name = db.Column(db.String(128))
    size = db.Column(db.Integer, nullable=False, index=True)
    snapshot_id = db.Column(None, db.ForeignKey("snapshot.id"), nullable=False)
    volume_id = db.Column(None, db.ForeignKey("volume.id"), nullable=False)
    status_id = db.Column(None,
                          db.ForeignKey("volume_status.id"),
                          nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "size", "snapshot_id", "volume_id",
                              "status_id"])


class VolumeAttachment(db.Model):
    id = id_column()
    instance = db.Column(db.String(64), nullable=False)
    volume_id = db.Column(None, db.ForeignKey("volume.id"), primary_key=True)
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            primary_key=True)

    def json(self):
        """JSON"""
        return to_dict(self, ["instance", "volume_id", "snapshot_id"])

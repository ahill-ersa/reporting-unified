# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name

from sqlalchemy import desc

from ersa_reporting import db, id_column
from ersa_reporting import get_db_binding

DB_BINDING = get_db_binding(__name__)


class Snapshot(db.Model):
    """A snapshot of the world."""
    __bind_key__ = DB_BINDING
    id = id_column()
    ts = db.Column(db.Integer, unique=True, nullable=False)
    mappings = db.relationship("AccountReferenceMapping", backref="snapshot")
    memberships = db.relationship("Membership", backref="snapshot")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "ts": self.ts}

    @classmethod
    def latest(cls, start_ts=0, end_ts=0):
        query = cls.query
        if start_ts > 0:
            query = query.filter(Snapshot.ts >= start_ts)
        if end_ts > 0:
            query = query.filter(Snapshot.ts < end_ts)

        result = query.order_by(desc(Snapshot.ts)).first()
        if result:
            return result.id
        else:
            return None


class Domain(db.Model):
    """An organisation-level domain."""
    __bind_key__ = DB_BINDING
    id = id_column()
    name = db.Column(db.String(128), unique=True, nullable=False)
    references = db.relationship("AccountReference", backref="domain")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class Tenant(db.Model):
    """OpenStack Tenant"""
    __bind_key__ = DB_BINDING
    id = id_column()
    openstack_id = db.Column(db.String(128), unique=True, nullable=False)
    allocation = db.Column(db.Integer)
    name = db.Column(db.String(128))
    description = db.Column(db.String(512))
    memberships = db.relationship("Membership", backref="tenant")

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "openstack_id": self.openstack_id,
            "allocation": self.allocation,
            "name": self.name,
            "description": self.description
        }

    @classmethod
    def in_domain(cls, name, start_ts=0, end_ts=0):
        """ Get list of tentants of a domain from a snapshot

            name: domain name
            start: start timestamp to filter snapshot
            end: end timestamp to filter snapshot
        """
        # Get the latest snapthot id in the range
        snapshot_id = Snapshot.latest(start_ts, end_ts)
        if snapshot_id is None:
            return []

        domain_query = Domain.query.filter(Domain.name == name).\
            with_entities(Domain.id).subquery()

        reference_query = AccountReference.query.\
            filter(AccountReference.domain_id.in_(domain_query)).\
            with_entities(AccountReference.id).subquery()

        mapping_query = AccountReferenceMapping.query.\
            filter(AccountReferenceMapping.snapshot_id == snapshot_id).\
            filter(AccountReferenceMapping.reference_id.in_(reference_query)).\
            with_entities(AccountReferenceMapping.account_id).subquery()

        membership_query = Membership.query.filter(Membership.snapshot_id == snapshot_id).\
            filter(Membership.account_id.in_(mapping_query)).\
            with_entities(Membership.tenant_id).subquery()

        tenant_query = Tenant.query.filter(Tenant.id.in_(membership_query)).\
            with_entities(Tenant.openstack_id, Tenant.allocation, Tenant.name, Tenant.description)

        return [{"openstack_id": item[0], "allocation": item[1], "name": item[2], "description": item[3]}
                for item in tenant_query.all()]


class Membership(db.Model):
    """Tenant Membership at a point-in-time."""
    __bind_key__ = DB_BINDING
    id = id_column()
    account_id = db.Column(None,
                           db.ForeignKey("account.id"),
                           index=True,
                           nullable=False)
    tenant_id = db.Column(None,
                          db.ForeignKey("tenant.id"),
                          index=True,
                          nullable=False)
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            index=True,
                            nullable=False)

    def json(self):
        """Jsonify"""

        return {
            "account": self.account_id,
            "tenant": self.tenant_id,
            "snapshot": self.snapshot_id
        }


class Account(db.Model):
    """OpenStack Account"""
    __bind_key__ = DB_BINDING
    id = id_column()
    openstack_id = db.Column(db.String(128), unique=True, nullable=False)
    mappings = db.relationship("AccountReferenceMapping", backref="account")
    memberships = db.relationship("Membership", backref="account")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "openstack_id": self.openstack_id}


class AccountReference(db.Model):
    """Email Address"""
    __bind_key__ = DB_BINDING
    id = id_column()
    value = db.Column(db.String(128), unique=True, nullable=False)
    domain_id = db.Column(None, db.ForeignKey("domain.id"))
    mappings = db.relationship("AccountReferenceMapping", backref="reference")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "value": self.value, "domain": self.domain_id}


class AccountReferenceMapping(db.Model):
    """Linkage between Email and OpenStack Account at point-in-time."""
    __bind_key__ = DB_BINDING
    id = id_column()
    account_id = db.Column(None, db.ForeignKey("account.id"),
                           nullable=False, index=True)
    reference_id = db.Column(None, db.ForeignKey("account_reference.id"),
                             nullable=False, index=True)
    snapshot_id = db.Column(None, db.ForeignKey("snapshot.id"), nullable=False)
    __table_args__ = (db.UniqueConstraint("account_id", "reference_id",
                                          "snapshot_id"), )

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "account": self.account_id,
            "reference": self.reference_id,
            "snapshot": self.snapshot_id
        }

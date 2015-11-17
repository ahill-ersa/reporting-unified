#!/usr/bin/python3
"""Application and persistence management."""

# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name

from functools import lru_cache

from ersa_reporting import db, id_column, configure, get_or_create, commit
from ersa_reporting import app, request, require_auth, QueryResource


def get_domain(name):
    """Extract an organisational domain from an email address."""
    if "@" in name:
        domain_name = name.split("@")[1]
        if domain_name.endswith(".edu.au"):
            domain_name = ".".join(domain_name.split(".")[-3:])
        elif domain_name.endswith(".edu"):
            domain_name = ".".join(domain_name.split(".")[-2:])
        return domain_name
    else:
        return None

# Data Models


class Snapshot(db.Model):
    """A snapshot of the world."""
    id = id_column()
    ts = db.Column(db.Integer, unique=True, nullable=False)
    mappings = db.relationship("AccountReferenceMapping", backref="snapshot")
    memberships = db.relationship("Membership", backref="snapshot")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "ts": self.ts}


class Domain(db.Model):
    """An organisation-level domain."""
    id = id_column()
    name = db.Column(db.String(128), unique=True, nullable=False)
    references = db.relationship("AccountReference", backref="domain")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class Tenant(db.Model):
    """OpenStack Tenant"""
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


class Membership(db.Model):
    """Tenant Membership at a point-in-time."""
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
    id = id_column()
    openstack_id = db.Column(db.String(128), unique=True, nullable=False)
    mappings = db.relationship("AccountReferenceMapping", backref="account")
    memberships = db.relationship("Membership", backref="account")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "openstack_id": self.openstack_id}


class AccountReference(db.Model):
    """Email Address"""
    id = id_column()
    value = db.Column(db.String(128), unique=True, nullable=False)
    domain_id = db.Column(None, db.ForeignKey("domain.id"))
    mappings = db.relationship("AccountReferenceMapping", backref="reference")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "value": self.value, "domain": self.domain_id}


class AccountReferenceMapping(db.Model):
    """Linkage between Email and OpenStack Account at point-in-time."""
    id = id_column()
    account_id = db.Column(None, db.ForeignKey("account.id"), nullable=False)
    reference_id = db.Column(None,
                             db.ForeignKey("account_reference.id"),
                             nullable=False)
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

# API


class AccountResource(QueryResource):
    """Account"""
    query_class = Account


class TenantResource(QueryResource):
    """Tenant"""
    query_class = Tenant


class DomainResource(QueryResource):
    """Domain"""
    query_class = Domain


class MembershipResource(QueryResource):
    """Membership"""
    query_class = Membership


class AccountReferenceResource(QueryResource):
    """Account Reference"""
    query_class = AccountReference


class AccountReferenceMappingResource(QueryResource):
    """Account Reference Mapping"""
    query_class = AccountReferenceMapping


class SnapshotResource(QueryResource):
    """Snapshot"""
    query_class = Snapshot

    @require_auth
    def put(self):
        """Ingest data."""

        @lru_cache(maxsize=10000)
        def cache(model, **kwargs):
            return get_or_create(model, **kwargs)

        for message in request.json:
            data = message["data"]

            snapshot = cache(Snapshot, ts=data["timestamp"])

            for account_detail in data["users"]:
                account = cache(Account, openstack_id=account_detail["id"])

                if not account_detail["email"]:
                    continue

                # Fix broken emails containing ";"
                email = account_detail["email"].split(";")[0]

                domain_name = get_domain(email)
                domain = cache(Domain,
                               name=domain_name) if domain_name else None

                reference = cache(AccountReference, value=email, domain=domain)
                cache(AccountReferenceMapping,
                      account=account,
                      reference=reference,
                      snapshot=snapshot)

            for tenant_detail in data["tenants"]:
                tenant = cache(Tenant, openstack_id=tenant_detail["id"])
                tenant.name = tenant_detail["name"]
                tenant.description = tenant_detail["description"]

                if "allocation_id" in tenant_detail:
                    try:
                        tenant.allocation = int(tenant_detail["allocation_id"])
                    except:
                        pass

                if "users" not in tenant_detail:
                    continue

                for member in tenant_detail["users"]:
                    account = cache(Account, openstack_id=member["id"])
                    cache(Membership,
                          account=account,
                          tenant=tenant,
                          snapshot=snapshot)

        commit()

        return "", 204


def setup():
    """Let's roll."""
    resources = {
        "/account": AccountResource,
        "/tenant": TenantResource,
        "/domain": DomainResource,
        "/membership": MembershipResource,
        "/reference": AccountReferenceResource,
        "/mapping": AccountReferenceMappingResource,
        "/snapshot": SnapshotResource
    }

    configure(resources)


setup()

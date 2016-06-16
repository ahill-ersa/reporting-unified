"""Application and persistence management."""

# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name

from functools import lru_cache

from ersa_reporting import configure, get_or_create
from ersa_reporting import record_input, commit, app, request
from ersa_reporting import require_auth, Resource, QueryResource
from ersa_reporting import BaseIngestResource

from nectar import get_domain
from .models.keystone import *

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


class IngestResource(BaseIngestResource):
    def ingest(self):
        """Ingest data."""

        @lru_cache(maxsize=10000)
        def cache(model, **kwargs):
            return get_or_create(model, **kwargs)

        for message in request.get_json(force=True):
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
        "/snapshot": SnapshotResource,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

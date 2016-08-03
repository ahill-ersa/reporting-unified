import uuid
from functools import lru_cache

from werkzeug.exceptions import NotFound

from flask_restful import reqparse
from flask_sqlalchemy import BaseQuery

from . import create_logger
from . import app, configure, request, require_auth
from . import db, get_or_create, add, commit, QUERY_PARSER, RANGE_PARSER
from . import QueryResource, BaseIngestResource, RangeQuery

from ..models.nova import (
    Snapshot, Image, Flavor, Hypervisor, AvailabilityZone, Tenant, Account,
    Instance, InstanceStatus, InstanceState, Summary,
    IPAddress, MACAddress, IPAddressMapping, MACAddressMapping
)

logger = create_logger(__name__)


class SnapshotResource(QueryResource):
    """Snapshot Endpoint"""
    query_class = Snapshot


class IngestResource(BaseIngestResource):
    def ingest(self):
        """Ingest data."""

        @lru_cache(maxsize=100000)
        def cache(model, **kwargs):
            return get_or_create(model, **kwargs)

        for message in request.get_json(force=True):
            data = message["data"]

            snapshot = cache(Snapshot, ts=data["timestamp"])

            for flavor_detail in data["flavors"]:
                flavor = cache(Flavor, openstack_id=flavor_detail["id"])
                flavor.name = flavor_detail["name"]
                flavor.vcpus = flavor_detail["vcpus"]
                flavor.ram = flavor_detail["ram"]
                flavor.disk = flavor_detail["disk"]
                flavor.ephemeral = flavor_detail["OS-FLV-EXT-DATA:ephemeral"]
                flavor.public = flavor_detail["os-flavor-access:is_public"]

            for instance_detail in data["instances"]:
                availability_zone_name = instance_detail[
                    "OS-EXT-AZ:availability_zone"]
                if not availability_zone_name:
                    continue
                availability_zone = cache(AvailabilityZone,
                                          name=availability_zone_name)
                if not availability_zone_name.startswith('sa'):
                    logger.debug("Skip non-sa zone: %s" % availability_zone_name)
                    continue

                hypervisor_hostname = instance_detail[
                    "OS-EXT-SRV-ATTR:hypervisor_hostname"]
                if not hypervisor_hostname:
                    continue

                hypervisor = cache(Hypervisor,
                                   name=hypervisor_hostname,
                                   availability_zone=availability_zone)

                flavor = cache(Flavor,
                               openstack_id=instance_detail["flavor"]["id"])
                account = cache(Account,
                                openstack_id=instance_detail["user_id"])
                tenant = cache(Tenant,
                               openstack_id=instance_detail["tenant_id"])
                status = cache(InstanceStatus,
                               name=instance_detail["OS-EXT-STS:vm_state"])

                if not isinstance(instance_detail["image"], dict):
                    continue
                image = cache(Image,
                              openstack_id=instance_detail["image"]["id"])

                instance = cache(Instance, openstack_id=instance_detail["id"])
                instance.account = account
                instance.tenant = tenant
                instance.flavor = flavor
                instance.availability_zone = availability_zone

                add(InstanceState(snapshot=snapshot,
                                  instance=instance,
                                  image=image,
                                  name=instance_detail["name"],
                                  hypervisor=hypervisor,
                                  status=status))

                for network in instance_detail["addresses"].values():
                    for address in network:
                        mac = cache(MACAddress,
                                    address=address["OS-EXT-IPS-MAC:mac_addr"])
                        add(MACAddressMapping(snapshot=snapshot,
                                              instance=instance,
                                              address=mac))

                        ip = cache(IPAddress,
                                   address=address["addr"],
                                   family=address["version"])
                        add(IPAddressMapping(snapshot=snapshot,
                                             instance=instance,
                                             address=ip))

        commit()
        return "", 204


class AccountResource(QueryResource):
    """Account Endpoint"""
    query_class = Account


class TenantResource(QueryResource):
    """Tenant Endpoint"""
    query_class = Tenant


class AvailabilityZoneResource(QueryResource):
    """AZ Endpoint"""
    query_class = AvailabilityZone


class IPAddressResource(QueryResource):
    """IP Address Endpoint"""
    query_class = IPAddress


class MACAddressResource(QueryResource):
    """MAC Address Endpoint"""
    query_class = MACAddress


class FlavorResource(QueryResource):
    """Flavor Endpoint"""
    query_class = Flavor


class HypervisorResource(QueryResource):
    """Hypervisor Endpoint"""
    query_class = Hypervisor


class InstanceLatestState(RangeQuery):
    """ Get the latest full information of an instance

        This includes its internal id, Instnace.openstack_id (server_id),
        InstanceState.name(server), Hypervisor.name (hypervisor),
        AvailabilityZone.name (az), Flavor.openstack_id (flavor),
        Image.openstack_id (image), life span (span), linked
        Account.openstack_id (account), Tenant.openstack_id (tenant).
    """
    def _get(self, id='', **kwargs):
        try:
            uuid.UUID(id)
        except:
            return {}

        self.default = rslt = {}
        instance = Instance.query.get(id)
        rslt = instance.latest_state(start_ts=kwargs['start'], end_ts=kwargs['end'])
        return rslt


class InstanceResource(QueryResource):
    """Instance Endpoint"""
    query_class = Instance

    # TODO: this should be removed after client code has to updated to use InstanceLatestState
    # this is to support old latest query
    def _latest_state(self):
        """ Get full information of an instance

            This includes its internal id, Instnace.openstack_id (server_id),
            InstanceState.name(server), Hypervisor.name (hypervisor),
            AvailabilityZone.name (az), Flavor.openstack_id (flavor),
            Image.openstack_id (image), life span (span), linked
            Account.openstack_id (account), Tenant.openstack_id (tenant).
        """
        parser = reqparse.RequestParser()
        parser.add_argument("id", required=True)
        parser.add_argument("start", type=int, default=0)
        parser.add_argument("end", type=int, default=0)
        args = parser.parse_args()

        instance = Instance.query.get(args["id"])
        return instance.latest_state(args["start"], args["end"])

    @require_auth
    def get(self):
        args = QUERY_PARSER.parse_args()
        if args["filter"]:
            return super(InstanceResource, self).get()
        else:
            return self._latest_state()


class InstanceStatusResource(QueryResource):
    """Instance Status Endpoint"""
    query_class = InstanceStatus


class ImageResource(QueryResource):
    """Image Endpoint"""
    query_class = Image


class InstanceStateResource(QueryResource):
    """Instance State Endpoint"""
    query_class = InstanceState


class SummaryResource(RangeQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for arg in self.arg_parser.args:
            arg.required = True
        self.arg_parser.add_argument("distinct", type=bool, default=False)

    # TODO: consider to remove
    def _query(self, start_ts, end_ts):
        """Build a query to get a list of all instance status and hypervisor bewteen sart and end ts."""

        # To use BaseQuery.paginate
        query = BaseQuery([Snapshot, InstanceState, Instance, Hypervisor, Account, Tenant, Flavor], db.session()).\
            filter(Snapshot.ts >= start_ts, Snapshot.ts < end_ts).\
            filter(InstanceState.snapshot_id == Snapshot.id).\
            filter(Instance.id == InstanceState.instance_id).\
            filter(InstanceState.hypervisor_id == Hypervisor.id).\
            filter(Account.id == Instance.account_id).\
            filter(Tenant.id == Instance.tenant_id).\
            filter(Flavor.id == Instance.flavor_id).\
            with_entities(Snapshot.ts, Instance.openstack_id, InstanceState.name,
                          Hypervisor.name, Account.openstack_id, Tenant.openstack_id,
                          Instance.flavor_id)
        return query

    def _get(self, **kwargs):
        common_args = QUERY_PARSER.parse_args()

        if kwargs["distinct"]:
            query = Summary(kwargs["start"], kwargs["end"]).query
        else:
            query = self._query(kwargs["start"], kwargs["end"])

        result = {'total': 0, 'pages': 0, 'items': []}
        try:
            qp = query.paginate(common_args["page"], common_args["count"])
            result['total'] = qp.total
            result['pages'] = qp.pages
            result['page'] = qp.page
            if kwargs["distinct"]:
                result['items'] = [item[0] for item in qp.items]
            else:
                result['items'] = [{
                    'ts': item[0], 'server_id': item[1], 'server': item[2],
                    'hypervisor': item[3], 'account': str(item[4]),
                    'tenant': str(item[5]), 'flavor': str(item[6])} for item in qp.items]
        except NotFound:
            pass
        except Exception as e:
            logger.error("Query of summary failed. Detail: %s" % str(e))

        return result


class IPAddressMappingResource(QueryResource):
    """IP Address Mapping Resource"""
    query_class = IPAddressMapping


class MACAddressMappingResource(QueryResource):
    """MAC Address Mapping Resource"""
    query_class = MACAddressMapping


def setup():
    """Let's roll."""
    resources = {
        "/snapshot": SnapshotResource,
        "/account": AccountResource,
        "/tenant": TenantResource,
        "/az": AvailabilityZoneResource,
        "/flavor": FlavorResource,
        "/hypervisor": HypervisorResource,
        "/image": ImageResource,
        "/instance": InstanceResource,
        "/instance/<id>/latest": InstanceLatestState,
        "/instance/status": InstanceStatusResource,
        "/instance/state": InstanceStateResource,
        "/summary": SummaryResource,
        "/ip": IPAddressResource,
        "/mac": MACAddressResource,
        "/ip/mapping": IPAddressMappingResource,
        "/mac/mapping": MACAddressMappingResource,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

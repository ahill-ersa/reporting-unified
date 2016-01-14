#!/usr/bin/python3
"""Application and persistence management."""

# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name

from functools import lru_cache

from sqlalchemy.dialects.postgresql import INET, MACADDR

from ersa_reporting import db, id_column, configure
from ersa_reporting import get_or_create, commit, app
from ersa_reporting import add, delete, request, require_auth
from ersa_reporting import Resource, QueryResource, record_input
from ersa_reporting import BaseIngestResource

# Data Models


class Snapshot(db.Model):
    """A snapshot of the world."""
    id = id_column()
    ts = db.Column(db.Integer, unique=True, nullable=False)
    instance_states = db.relationship("InstanceState", backref="snapshot")
    ip_address_mappings = db.relationship("IPAddressMapping",
                                          backref="snapshot")
    mac_address_mappings = db.relationship("MACAddressMapping",
                                           backref="snapshot")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "ts": self.ts}


class Account(db.Model):
    """OpenStack Account"""
    id = id_column()
    openstack_id = db.Column(db.String(128), unique=True, nullable=False)
    instances = db.relationship("Instance", backref="account")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "openstack_id": self.openstack_id}


class Tenant(db.Model):
    """OpenStack Tenant"""
    id = id_column()
    openstack_id = db.Column(db.String(128), unique=True, nullable=False)
    instances = db.relationship("Instance", backref="tenant")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "openstack_id": self.openstack_id}


class AvailabilityZone(db.Model):
    """OpenStack AZ/Cell"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    hypervisors = db.relationship("Hypervisor", backref="availability_zone")
    instances = db.relationship("Instance", backref="availability_zone")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class IPAddress(db.Model):
    """IP Address (v4 or v6)"""
    id = id_column()
    address = db.Column(INET, unique=True, nullable=False)
    family = db.Column(db.Integer, index=True, nullable=False)
    mappings = db.relationship("IPAddressMapping", backref="address")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "address": self.address, "family": self.family}


class MACAddress(db.Model):
    """MAC Address"""
    id = id_column()
    address = db.Column(MACADDR, unique=True, nullable=False)
    mappings = db.relationship("MACAddressMapping", backref="address")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "address": self.address}


class Flavor(db.Model):
    """OpenStack Flavor"""
    id = id_column()
    openstack_id = db.Column(db.String(128), unique=True, nullable=False)
    name = db.Column(db.String(64), unique=True, nullable=False)
    vcpus = db.Column(db.Integer)
    ram = db.Column(db.Integer)
    disk = db.Column(db.Integer)
    ephemeral = db.Column(db.Integer)
    public = db.Column(db.Boolean)
    instances = db.relationship("Instance", backref="flavor")

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "openstack_id": self.openstack_id,
            "name": self.name,
            "vcpus": self.vcpus,
            "ram": self.ram,
            "disk": self.disk,
            "ephemeral": self.ephemeral,
            "public": self.public
        }


class Hypervisor(db.Model):
    """OpenStack Hypervisor"""
    id = id_column()
    name = db.Column(db.String(128), unique=True, nullable=False)
    availability_zone_id = db.Column(None,
                                     db.ForeignKey("availability_zone.id"),
                                     nullable=False)
    instance_states = db.relationship("InstanceState", backref="hypervisor")

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "name": self.name,
            "availability_zone": self.availability_zone_id
        }


class Instance(db.Model):
    """OpenStack VM/Instance"""
    id = id_column()
    openstack_id = db.Column(db.String(128), unique=True, nullable=False)
    availability_zone_id = db.Column(None,
                                     db.ForeignKey("availability_zone.id"),
                                     index=True,
                                     nullable=False)
    account_id = db.Column(None,
                           db.ForeignKey("account.id"),
                           index=True,
                           nullable=False)
    tenant_id = db.Column(None,
                          db.ForeignKey("tenant.id"),
                          index=True,
                          nullable=False)
    flavor_id = db.Column(None,
                          db.ForeignKey("flavor.id"),
                          index=True,
                          nullable=False)
    instance_states = db.relationship("InstanceState", backref="instance")
    ip_addresses = db.relationship("IPAddressMapping", backref="instance")
    mac_addresses = db.relationship("MACAddressMapping", backref="instance")

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "openstack_id": self.openstack_id,
            "account": self.account_id,
            "tenant": self.tenant_id,
            "flavor": self.flavor_id
        }


class InstanceStatus(db.Model):
    """Instance States (e.g. active, error, etc.)"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    instance_state = db.relationship("InstanceState", backref="status")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class Image(db.Model):
    """OpenStack Images"""
    id = id_column()
    openstack_id = db.Column(db.String(64), unique=True, nullable=False)
    instance_state = db.relationship("InstanceState", backref="image")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "openstack_id": self.openstack_id}


class InstanceState(db.Model):
    """Point-in-time OpenStack Instance State"""
    id = id_column()
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            index=True,
                            nullable=False)
    instance_id = db.Column(None,
                            db.ForeignKey("instance.id"),
                            index=True,
                            nullable=False)
    image_id = db.Column(None, db.ForeignKey("image.id"), nullable=False)
    status_id = db.Column(None,
                          db.ForeignKey("instance_status.id"),
                          nullable=False)
    hypervisor_id = db.Column(None,
                              db.ForeignKey("hypervisor.id"),
                              nullable=False)

    name = db.Column(db.String(128), index=True, nullable=False)

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "name": self.name,
            "snapshot": self.snapshot_id,
            "instance": self.instance_id,
            "image": self.image_id,
            "status": self.status_id,
            "hypervisor": self.hypervisor_id
        }


class IPAddressMapping(db.Model):
    """Point-in-time IP-Instance Mapping"""
    id = id_column()
    instance_id = db.Column(None,
                            db.ForeignKey("instance.id"),
                            index=True,
                            nullable=False)
    address_id = db.Column(None,
                           db.ForeignKey("ip_address.id"),
                           index=True,
                           nullable=False)
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            index=True,
                            nullable=False)

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "instance": self.instance_id,
            "address": self.address_id,
            "snapshot": self.snapshot_id
        }


class MACAddressMapping(db.Model):
    """Point-in-time MAC-Instance Mapping"""
    id = id_column()
    instance_id = db.Column(None,
                            db.ForeignKey("instance.id"),
                            index=True,
                            nullable=False)
    address_id = db.Column(None,
                           db.ForeignKey("mac_address.id"),
                           index=True,
                           nullable=False)
    snapshot_id = db.Column(None,
                            db.ForeignKey("snapshot.id"),
                            index=True,
                            nullable=False)

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "instance": self.instance_id,
            "address": self.address_id,
            "snapshot": self.snapshot_id
        }

# Endpoints


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


class InstanceResource(QueryResource):
    """Instance Endpoint"""
    query_class = Instance


class InstanceStatusResource(QueryResource):
    """Instance Status Endpoint"""
    query_class = InstanceStatus


class ImageResource(QueryResource):
    """Image Endpoint"""
    query_class = Image


class InstanceStateResource(QueryResource):
    """Instance State Endpoint"""
    query_class = InstanceState


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
        "/instance/status": InstanceStatusResource,
        "/instance/state": InstanceStateResource,
        "/ip": IPAddressResource,
        "/mac": MACAddressResource,
        "/ip/mapping": IPAddressMappingResource,
        "/mac/mapping": MACAddressMappingResource,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

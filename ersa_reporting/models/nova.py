# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name

from flask_sqlalchemy import BaseQuery
from sqlalchemy.dialects.postgresql import INET, MACADDR
from sqlalchemy import distinct, desc
from sqlalchemy.sql import func

from ersa_reporting import db, id_column
from ersa_reporting import get_db_binding

DB_BINDING = get_db_binding(__name__)

class Account(db.Model):
    """OpenStack Account"""
    __bind_key__ = DB_BINDING
    id = id_column()
    openstack_id = db.Column(db.String(128), unique=True, nullable=False)
    instances = db.relationship("Instance", backref="account")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "openstack_id": self.openstack_id}


class Tenant(db.Model):
    """OpenStack Tenant"""
    __bind_key__ = DB_BINDING
    id = id_column()
    openstack_id = db.Column(db.String(128), unique=True, nullable=False)
    instances = db.relationship("Instance", backref="tenant")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "openstack_id": self.openstack_id}


class AvailabilityZone(db.Model):
    """OpenStack AZ/Cell"""
    __bind_key__ = DB_BINDING
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    hypervisors = db.relationship("Hypervisor", backref="availability_zone")
    instances = db.relationship("Instance", backref="availability_zone")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class Instance(db.Model):
    """OpenStack VM/Instance"""
    __bind_key__ = DB_BINDING
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

    def latest_state(self, start, end):
        """ Get full information of the latest state of an instance in the query date range

            This includes its internal instance_id, Instnace.openstack_id (server_id),
            InstanceState.name(server), Hypervisor.name (hypervisor),
            AvailabilityZone.name (az), Flavor.openstack_id (flavor),
            Image.openstack_id (image), life span (span), linked
            Account.openstack_id (account), Tenant.openstack_id (tenant).
        """
        state = InstanceState.latest(self.id, start, end)

        state["instance_id"] = self.id
        state["server_id"] = self.openstack_id

        state['account'] = Account.query.get(self.account_id).openstack_id
        state['tenant'] = Tenant.query.get(self.tenant_id).openstack_id
        state['flavor'] = Flavor.query.get(self.flavor_id).openstack_id
        state['az'] = AvailabilityZone.query.get(self.availability_zone_id).name

        return state

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "openstack_id": self.openstack_id,
            "account": self.account_id,
            "tenant": self.tenant_id,
            "flavor": self.flavor_id
        }

class Snapshot(db.Model):
    """A snapshot of the world."""
    __bind_key__ = DB_BINDING
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

class IPAddress(db.Model):
    """IP Address (v4 or v6)"""
    __bind_key__ = DB_BINDING
    id = id_column()
    address = db.Column(INET, unique=True, nullable=False)
    family = db.Column(db.Integer, index=True, nullable=False)
    mappings = db.relationship("IPAddressMapping", backref="address")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "address": self.address, "family": self.family}


class MACAddress(db.Model):
    """MAC Address"""
    __bind_key__ = DB_BINDING
    id = id_column()
    address = db.Column(MACADDR, unique=True, nullable=False)
    mappings = db.relationship("MACAddressMapping", backref="address")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "address": self.address}


class Flavor(db.Model):
    """OpenStack Flavor"""
    __bind_key__ = DB_BINDING
    id = id_column()
    openstack_id = db.Column(db.String(128), unique=True, nullable=False)
    name = db.Column(db.String(64), nullable=False)
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
    __bind_key__ = DB_BINDING
    id = id_column()
    name = db.Column(db.String(128), nullable=False)
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


class InstanceStatus(db.Model):
    """Instance States (e.g. active, error, etc.)"""
    __bind_key__ = DB_BINDING
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    instance_state = db.relationship("InstanceState", backref="status")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class Image(db.Model):
    """OpenStack Images"""
    __bind_key__ = DB_BINDING
    id = id_column()
    openstack_id = db.Column(db.String(64), unique=True, nullable=False)
    instance_state = db.relationship("InstanceState", backref="image")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "openstack_id": self.openstack_id}


class InstanceState(db.Model):
    """Point-in-time OpenStack Instance State"""
    __bind_key__ = DB_BINDING
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

    @classmethod
    def latest(cls, instance_id, start_ts=0, end_ts=0):
        """" Get the latest information of an instance in a given time range.

             During the life time of an instance, image or hypervisor can be
             changed, only report back the latest one.
        """
        # This retrieves all InstanceState.id and Snapshot.ts in order
        # to get the latest state and calculate span at once.
        # This sacrifices memory usage to avoid multiple database hit.
        query = db.session.query(InstanceState).join(Snapshot).\
                  filter(InstanceState.snapshot_id == Snapshot.id).\
                  filter(InstanceState.instance_id == instance_id).\
                  order_by(desc(Snapshot.ts))

        if start_ts > 0:
            query = query.filter(Snapshot.ts >= start_ts)
        if end_ts > 0:
            query = query.filter(Snapshot.ts < end_ts)

        timely_states = query.with_entities(InstanceState.id, Snapshot.ts)
        latest_state = InstanceState.query.get(timely_states[0][0])
        state = {"server": latest_state.name}
        # state["span"] is the difference between mapped snapshots
        # The accuracy depends on snapshot resolution
        state["span"] = timely_states[0][1] - timely_states[-1][1]
        state["image"] = latest_state.image.openstack_id
        state["hypervisor"] = Hypervisor.query.filter_by(id=latest_state.hypervisor_id).value("name")

        return state



class IPAddressMapping(db.Model):
    """Point-in-time IP-Instance Mapping"""
    __bind_key__ = DB_BINDING
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
    __bind_key__ = DB_BINDING
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

class Summary():
    """list distinct instance on sa node between start_ts and end_ts """
    query = None

    def __init__(self, start_ts, end_ts):
        """Build a query to get a distinct list of instance_id bewteen sart and end ts."""
        az_query = db.session.query(Hypervisor).join(AvailabilityZone).\
                     filter(Hypervisor.availability_zone_id == AvailabilityZone.id).\
                     filter(AvailabilityZone.name.like("sa%")).\
                     with_entities(Hypervisor.id).subquery()

        self.query = BaseQuery([Snapshot, InstanceState], db.session()).\
                  filter(Snapshot.ts >= start_ts, Snapshot.ts < end_ts).\
                  filter(InstanceState.snapshot_id == Snapshot.id).\
                  with_entities(InstanceState.instance_id).\
                  distinct(InstanceState.instance_id).\
                  filter(InstanceState.hypervisor_id.in_(az_query))

    def value(self):
        return [item[0] for item in self.query.all()]

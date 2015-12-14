#!/usr/bin/env python3
"""Application and persistence management."""

# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name

from sqlalchemy.dialects.postgresql import UUID

from ersa_reporting import app, db, add, id_column, configure, commit
from ersa_reporting import get_or_create, record_input
from ersa_reporting import request, require_auth, Resource, QueryResource

# Data Models


class Snapshot(db.Model):
    """Snapshot Data Model"""
    id = id_column()
    ts = db.Column(db.Integer, nullable=False)

    person_email = db.relationship("PersonEmail", backref="snapshot")
    person_username = db.relationship("PersonUsername", backref="snapshot")
    memberships = db.relationship("Membership", backref="snapshot")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "ts": self.ts}


class Organisation(db.Model):
    """Organisation Data Model"""
    id = id_column()
    insightly_id = db.Column(db.Integer, index=True, nullable=False)
    name = db.Column(db.String(256), index=True)
    membership = db.relationship("Membership", backref="organisation")

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "insightly_id": self.insightly_id,
            "name": self.name
        }


class Person(db.Model):
    """Person Data Model"""
    id = id_column()
    insightly_id = db.Column(db.Integer, index=True, nullable=False)
    first_name = db.Column(db.String(128), index=True)
    last_name = db.Column(db.String(128), index=True)
    email = db.relationship("PersonEmail", backref="person")
    username = db.relationship("PersonUsername", backref="person")
    membership = db.relationship("Membership", backref="person")

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "insightly_id": self.insightly_id,
            "first_name": self.first_name,
            "last_name": self.last_name
        }


class Email(db.Model):
    """Email Data Model"""
    id = id_column()
    address = db.Column(db.String(128), index=True, nullable=False)
    person = db.relationship("PersonEmail", backref="email")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "address": self.address}


class PersonEmail(db.Model):
    """Person-Email Mapping Data Model"""
    id = id_column()
    person_id = db.Column(None, db.ForeignKey("person.id"))
    email_id = db.Column(None, db.ForeignKey("email.id"))
    snapshot_id = db.Column(None, db.ForeignKey("snapshot.id"))

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "person": self.person_id,
            "email": self.email_id,
            "snapshot": self.snapshot_id
        }


class Username(db.Model):
    """Username Data Model"""
    id = id_column()
    username = db.Column(db.String(64), index=True, nullable=False)
    person = db.relationship("PersonUsername", backref="username")

    def json(self):
        """Jsonify"""
        return {"id": self.id, "username": self.username}


class PersonUsername(db.Model):
    """Person-Username Mapping Data Model"""
    id = id_column()
    person_id = db.Column(None, db.ForeignKey("person.id"))
    username_id = db.Column(None, db.ForeignKey("username.id"))
    snapshot_id = db.Column(None, db.ForeignKey("snapshot.id"))

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "person": self.person_id,
            "username": self.username_id,
            "snapshot": self.snapshot_id
        }


class Membership(db.Model):
    """Organisation Membership Data Model"""
    id = id_column()
    person_id = db.Column(None, db.ForeignKey("person.id"))
    organisation_id = db.Column(None, db.ForeignKey("organisation.id"))
    snapshot_id = db.Column(None, db.ForeignKey("snapshot.id"))

    def json(self):
        """Jsonify"""
        return {
            "id": self.id,
            "person": self.person_id,
            "organisation": self.organisation_id,
            "snapshot": self.snapshot_id
        }

# Endpoints


class SnapshotResource(QueryResource):
    """Snapshot Endpoint"""
    query_class = Snapshot


class IngestResource(Resource):
    @require_auth
    def put(self):
        """Ingest snapshots."""

        record_input()

        for message in request.json:
            data = message["data"]

            snapshot = Snapshot(ts=data["timestamp"])
            add(snapshot)

            for entry in data["organisations"]:
                organisation = get_or_create(Organisation,
                                             insightly_id=entry["id"])
                organisation.name = entry["name"]

            for entry in data["contacts"]:
                person = get_or_create(Person, insightly_id=entry["id"])
                person.first_name = entry["first_name"]
                person.last_name = entry["last_name"]

                if entry["username"]:
                    username = get_or_create(Username,
                                             username=entry["username"])
                    get_or_create(PersonUsername,
                                  snapshot=snapshot,
                                  person=person,
                                  username=username)

                if entry["email"]:
                    for address in entry["email"]:
                        email = get_or_create(Email, address=address)
                        get_or_create(PersonEmail,
                                      snapshot=snapshot,
                                      person=person,
                                      email=email)

                if entry["organisations"]:
                    for insightly_id in entry["organisations"]:
                        organisation = get_or_create(Organisation,
                                                     insightly_id=insightly_id)
                        get_or_create(Membership,
                                      snapshot=snapshot,
                                      organisation=organisation,
                                      person=person)

        commit()

        return "", 204


class OrganisationResource(QueryResource):
    """Organisation Endpoint"""
    query_class = Organisation


class PersonResource(QueryResource):
    """Person Endpoint"""
    query_class = Person


class EmailResource(QueryResource):
    """Email Endpoint"""
    query_class = Email


class PersonEmailResource(QueryResource):
    """Person/Email Endpoint"""
    query_class = PersonEmail


class UsernameResource(QueryResource):
    """Username Endpoint"""
    query_class = Username


class PersonUsernameResource(QueryResource):
    """Person/Username Endpoint"""
    query_class = PersonUsername


class MembershipResource(QueryResource):
    """Membership Endpoint"""
    query_class = Membership


def setup():
    """Let's roll."""

    resources = {
        "/snapshot": SnapshotResource,
        "/person": PersonResource,
        "/email": EmailResource,
        "/person-email": PersonEmailResource,
        "/username": UsernameResource,
        "/person-username": PersonUsernameResource,
        "/organisation": OrganisationResource,
        "/membership": MembershipResource,
        "/ingest": IngestResource
    }

    configure(resources)


setup()

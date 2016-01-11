#!/usr/bin/env python3
"""Base Flask"""

# pylint: disable=no-init, too-few-public-methods, no-self-use

import os
import re
import sys
import uuid

from functools import wraps

import requests

from flask import Flask, request
from flask.ext import restful
from flask.ext.cors import CORS
from flask.ext.restful import Resource, reqparse
from flask.ext.sqlalchemy import SQLAlchemy

from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm.relationships import RelationshipProperty

QUERY_PARSER = reqparse.RequestParser()
QUERY_PARSER.add_argument("filter", action="append", help="Filter")
QUERY_PARSER.add_argument("order", help="Ordering", default="id")
QUERY_PARSER.add_argument("page", type=int, default=1, help="Page #")
QUERY_PARSER.add_argument("count",
                          type=int,
                          default=1000,
                          help="Items per page")

INPUT_PARSER = reqparse.RequestParser()
INPUT_PARSER.add_argument("name", location="args", required=True)

PACKAGE = os.environ["ERSA_REPORTING_PACKAGE"]

STRIP_ID = re.compile("_id$")

REQUIRED_ENVIRONMENT = ["ERSA_BIND", "ERSA_DATABASE_URI"]

UUID_NAMESPACE = uuid.UUID("aeb7cf1c-a842-4592-82e9-55d2dad00150")

app = Flask("app")

# Stop SQLAlchemy complaining, re: "SQLALCHEMY_TRACK_MODIFICATIONS adds
# significant overhead and will be disabled by default in the future."
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

cors = CORS(app)
restapi = restful.Api(app)
db = SQLAlchemy(app)


def identifier(content):
    """A generator for consistent IDs."""
    return str(uuid.uuid5(UUID_NAMESPACE, str(content)))


def register_input(name):
    """
    Register an input.
    Concurrent modifications will fail on commit and a retry
    will return Conflict (HTTP 409), so this is safe.
    """
    item = get(Input, name=name)
    if item:
        return "", 409
    else:
        add(Input(name=name))
        return None


def missing_environment(extras=None):
    """Check for missing environment variables."""
    required_environment = REQUIRED_ENVIRONMENT.copy()
    if extras and len(extras) > 0:
        required_environment += extras

    missing = [var for var in required_environment if var not in os.environ]

    if len(missing) > 0:
        return "Missing environment vars: %s" % " ".join(missing)
    else:
        return None


def github(deps):
    """
    Format GitHub dependencies. For example:
    deps = [
        ("eresearchsa/flask-util", "ersa-flask-util", "0.4"),
        ("foo/bar", "my-package-name", "3.141")
    ]
    """
    return ["https://github.com/%s/archive/v%s.tar.gz#egg=%s-%s" %
            (dep[0], dep[2], dep[1], dep[2]) for dep in deps]


def get_or_create(model, **kwargs):
    """Fetch object if returned by filter query, else create new."""
    item = get(model, **kwargs)
    if not item:
        item = model(**kwargs)
        db.session.add(item)
    return item


def get(model, **kwargs):
    """Fetch object by query parameters."""
    return db.session.query(model).filter_by(**kwargs).first()


def commit():
    """Commit session."""
    db.session.commit()


def rollback():
    """Rollback session."""
    db.session.rollback()


def add(item):
    """Add object."""
    db.session.add(item)


def delete(item):
    """Delete object."""
    db.session.delete(item)


def fetch(model, key):
    """Fetch by ID."""
    return db.session.query(model).get(key)


def flush():
    """Flush session."""
    db.session.flush()


def require_auth(func):
    """
    Authenticate via the external reporting-auth service.
    """

    @wraps(func)
    def decorated(*args, **kwargs):
        """Check the header."""
        token = str(uuid.UUID(request.headers.get("x-ersa-auth-token", "")))
        auth_response = requests.get("https://reporting.ersa.edu.au/auth?secret=%s" % token)
        if auth_response.status_code != 200:
            return "", 403
        else:
            auth_data = auth_response.json()
            for endpoint in auth_data["endpoints"]:
                if endpoint["name"] == PACKAGE:
                    return func(*args, **kwargs)
            return "", 403

    return decorated


def id_column():
    """Generate a UUID column."""
    return db.Column(UUID,
                     server_default=text("uuid_generate_v4()"),
                     primary_key=True)


def to_dict(object, fields):
    """Generate dictionary with specified fields."""
    output = {}
    fields = set(["id"] + (fields if fields is not None else []))
    for name in fields:
        if hasattr(object, name):
            output[STRIP_ID.sub("", name)] = getattr(object, name)
    return output


def dynamic_query(model, query, expression):
    """
    Construct query based on:
        attribute.operation.expression
    For example:
        foo.eq.42
    """
    key, op, value = expression.split(".", 2)
    column = getattr(model, key, None)
    if isinstance(column.property, RelationshipProperty):
        column = getattr(model, key + "_id", None)
    if op == "in":
        query_filter = column.in_(value.split(","))
    else:
        attr = None
        for candidate in ["%s", "%s_", "__%s__"]:
            if hasattr(column, candidate % op):
                attr = candidate % op
                break
        if value == "null":
            value = None
        query_filter = getattr(column, attr)(value)
    return query.filter(query_filter)


def name_or_id(model, name):
    """Return an _id attribute if one exists."""
    name_id = name + "_id"
    if hasattr(model, name_id):
        return getattr(model, name_id)
    elif hasattr(model, name):
        return getattr(model, name)
    else:
        return None


def do_query(model):
    """Perform a query with request-specified filtering and ordering."""
    args = QUERY_PARSER.parse_args()
    query = model.query
    # filter
    if args["filter"]:
        for query_filter in args["filter"]:
            query = dynamic_query(model, query, query_filter)
    # order
    order = []
    for order_spec in args["order"].split(","):
        if not order_spec.startswith("-"):
            order.append(name_or_id(model, order_spec))
        else:
            order.append(name_or_id(model, order_spec[1:]).desc())
    query = query.order_by(*order)
    # execute
    return query.paginate(args["page"], per_page=args["count"]).items


def record_input():
    """Record the name of an ingestion."""
    args = INPUT_PARSER.parse_args()
    add(Input(name=args["name"]))


class QueryResource(Resource):
    """Generic Query"""

    def get_raw(self):
        """Query"""
        return do_query(self.query_class)

    @require_auth
    def get(self):
        """Query"""
        return [item.json() for item in self.get_raw()]

    @require_auth
    def post(self):
        return self.get()


class Input(db.Model):
    """Input"""
    id = id_column()
    name = db.Column(db.String(256), nullable=False, unique=True)

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class InputResource(QueryResource):
    """Input"""
    query_class = Input

    @require_auth
    def put(self):
        """Record a processed input."""
        record_input()
        commit()
        return "", 204


class PingResource(Resource):
    """Basic liveness test."""

    def get(self):
        """Hello?"""
        return "pong"


def configure(resources):
    restapi.add_resource(PingResource, "/ping")
    restapi.add_resource(InputResource, "/input")

    for (endpoint, cls) in resources.items():
        restapi.add_resource(cls, endpoint)


app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["ERSA_DATABASE_URI"]
app.config["DEBUG"] = os.getenv("ERSA_DEBUG", "").lower() == "true"

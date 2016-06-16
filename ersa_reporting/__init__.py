#!/usr/bin/env python3
"""Base Flask"""

# pylint: disable=no-init, too-few-public-methods, no-self-use

import re
import uuid

from functools import wraps

import requests
import logging
import logging.handlers

from flask import Flask, request

from flask_cors import CORS

import flask_restful
from flask_restful import Resource, reqparse

from flask_sqlalchemy import SQLAlchemy
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

app = Flask("app")
app.config.from_object('config')

PACKAGE = app.config["ERSA_REPORTING_PACKAGE"]

STRIP_ID = re.compile("_id$")

# TODO: to remove
# REQUIRED_ENVIRONMENT = ["ERSA_BIND", "ERSA_DATABASE_URI"]

AUTH_TOKEN = app.config["ERSA_AUTH_TOKEN"]
if AUTH_TOKEN is not None:
    AUTH_TOKEN = AUTH_TOKEN.lower()

UUID_NAMESPACE = uuid.UUID("aeb7cf1c-a842-4592-82e9-55d2dad00150")

if "LOG_DIR" in app.config:
    LOG_DIR = app.config["LOG_DIR"]
else:
    LOG_DIR = "."

if "LOG_LEVEL" in app.config:
    LOG_LEVEL = getattr(logging, app.config["LOG_LEVEL"].upper(), logging.DEBUG)
else:
    LOG_LEVEL = logging.DEBUG

if "LOG_SIZE" in app.config:
    LOG_SIZE = app.config["LOG_SIZE"]
else:
    LOG_SIZE = 30000000

top_logger = logging.getLogger(__name__)


# Logger is created by the calling module with the calling module's name as log name
# All other modules use this log
def create_logger(module_name):
    log_name = "%s/%s.log" % (LOG_DIR, module_name)
    file_handler = logging.handlers.RotatingFileHandler(log_name, maxBytes=LOG_SIZE)
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger = logging.getLogger(__name__)
    logger.addHandler(file_handler)
    logger.setLevel(LOG_LEVEL)

    return logger


def get_db_binding(package):
    """Get db binding for a package. Default is None. Argument is __name__"""
    db_binding = None
    if app.config["SQLALCHEMY_BINDS"]:
        MOD = package.split(".")[-1]
        if MOD in app.config["SQLALCHEMY_BINDS"]:
            db_binding = MOD
    return db_binding


# Stop SQLAlchemy complaining, re: "SQLALCHEMY_TRACK_MODIFICATIONS adds
# significant overhead and will be disabled by default in the future."
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

cors = CORS(app)
restapi = flask_restful.Api(app)
db = SQLAlchemy(app)


def identifier(content):
    """A generator for consistent IDs."""
    return str(uuid.uuid5(UUID_NAMESPACE, str(content)))


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


def constant_time_compare(val1, val2):
    """
    Borrowed from Django!

    Returns True if the two strings are equal, False otherwise.
    The time taken is independent of the number of characters that match.
    For the sake of simplicity, this function executes in constant time only
    when the two strings have the same length. It short-circuits when they
    have different lengths. Since Django only uses it to compare hashes of
    known expected length, this is acceptable.
    """
    if len(val1) != len(val2):
        return False
    result = 0
    for x, y in zip(val1, val2):
        result |= ord(x) ^ ord(y)
    return result == 0


def require_auth(func):
    """
    Authenticate via the external reporting-auth service.

    For dev/test purposes: if ERSA_AUTH_TOKEN environment variable
    exists, check against that instead.
    """

    @wraps(func)
    def decorated(*args, **kwargs):
        """Check the header."""
        success = False

        try:
            token = str(uuid.UUID(request.headers.get("x-ersa-auth-token",
                                                      ""))).lower()
        except:
            return "", 403

        if AUTH_TOKEN is not None:
            if constant_time_compare(token, AUTH_TOKEN):
                success = True
        else:
            auth_response = requests.get(
                "https://reporting.ersa.edu.au/auth?secret=%s" % token)
            if auth_response.status_code == 200:
                auth_data = auth_response.json()
                for endpoint in auth_data["endpoints"]:
                    if endpoint["name"] == PACKAGE:
                        success = True
                        break

        if success:
            return func(*args, **kwargs)
        else:
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
    return query.paginate(args["page"], per_page=args["count"], error_out=False).items


def record_input():
    """Record the name of an ingestion."""
    args = INPUT_PARSER.parse_args()
    add(Input(name=args["name"]))


class QueryResource(Resource):
    """Generic Query"""

    def get_raw(self):
        """Query"""
        try:
            top_logger.debug("Query: %s" % self.query_class.query)
            return do_query(self.query_class)
        except Exception as e:
            top_logger.error("Query %s failed. Detail: %s" % (self.query_class.query, str(e)))
            return []

    @require_auth
    def get(self):
        """Query"""
        return [item.json() for item in self.get_raw()]

    @require_auth
    def post(self):
        return self.get()


class BaseIngestResource(Resource):
    """Base Ingestion"""

    @require_auth
    def put(self):
        record_input()
        return self.ingest()


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

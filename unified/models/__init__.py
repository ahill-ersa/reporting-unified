import re

from sqlalchemy.sql import text
from sqlalchemy.orm import load_only
from sqlalchemy.dialects.postgresql import UUID

from .. import app, db

STRIP_ID = re.compile("_id$")


def to_dict(object, fields):
    """Generate dictionary with specified fields."""
    output = {}
    fields = set(["id"] + (fields if fields is not None else []))
    for name in fields:
        if hasattr(object, name):
            output[STRIP_ID.sub("", name)] = getattr(object, name)
    return output


def id_column():
    """Generate a UUID column."""
    return db.Column(UUID,
                     server_default=text("uuid_generate_v4()"),
                     primary_key=True)


def get_db_binding(package):
    """Get db binding for a package. Default is None. Argument is __name__"""
    db_binding = None
    if app.config["SQLALCHEMY_BINDS"]:
        MOD = package.split(".")[-1]
        if MOD in app.config["SQLALCHEMY_BINDS"]:
            db_binding = MOD
    return db_binding


class Input(db.Model):
    """Input"""
    id = id_column()
    name = db.Column(db.String(256), nullable=False, unique=True)

    def json(self):
        """Jsonify"""
        return {"id": self.id, "name": self.name}


class SnapshotMothods(object):
    """Mixin for Snapshot"""
    @classmethod
    def id_between(cls, start_ts=0, end_ts=0):
        """"Gets snapshop ids between start_ts and end_ts.

        It returns a subquery not actual values.
        """
        id_query = cls.query
        if start_ts > 0:
            id_query = id_query.filter(cls.ts >= start_ts)
        if end_ts > 0:
            id_query = id_query.filter(cls.ts < end_ts)
        return id_query.with_entities(cls.id).subquery()

    @classmethod
    def between(cls, start_ts=0, end_ts=0):
        """"Gets snapshop id and timestamps between start_ts and end_ts.

        It returns a subquery not actual values.
        """
        btw_query = cls.query
        if start_ts > 0:
            btw_query = btw_query.filter(cls.ts >= start_ts)
        if end_ts > 0:
            btw_query = btw_query.filter(cls.ts < end_ts)
        return btw_query.options(load_only("id", "ts")).subquery()

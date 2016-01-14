#!/usr/bin/python3
"""Application and persistence management."""

# pylint: disable=no-member, import-error, no-init, too-few-public-methods
# pylint: disable=cyclic-import, no-name-in-module, invalid-name

from ersa_reporting import app, db, configure, get_or_create, commit, to_dict
from ersa_reporting import request, id_column, require_auth, QueryResource
from ersa_reporting import fetch, delete, rollback

# Data Models


class EntityType(db.Model):
    """Entity Type"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    entities = db.relationship("Entity", backref="type")
    relationships1 = db.relationship(
        "EntityRelationship",
        foreign_keys="EntityRelationship.valid1_id",
        backref="valid1")
    relationships2 = db.relationship(
        "EntityRelationship",
        foreign_keys="EntityRelationship.valid2_id",
        backref="valid2")
    integer_attributes = db.relationship("EntityIntegerAttribute",
                                         backref="type")
    float_attributes = db.relationship("EntityFloatAttribute", backref="type")
    string_attributes = db.relationship("EntityStringAttribute",
                                        backref="type")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class EntityName(db.Model):
    """Entity Name"""
    id = id_column()
    name = db.Column(db.String(256), unique=True, nullable=False)
    mappings = db.relationship("EntityNameMapping", backref="name")

    def json(self):
        """JSON"""
        return to_dict(self, ["name"])


class EntityRelationship(db.Model):
    """Entity Relationship"""
    id = id_column()
    valid1_id = db.Column(None, db.ForeignKey("entity_type.id"))
    valid2_id = db.Column(None, db.ForeignKey("entity_type.id"))
    name = db.Column(db.String(64), unique=True, nullable=False)

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "valid1_id", "valid2_id"])


class EntityIntegerAttribute(db.Model):
    """Entity Attribute"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    type_id = db.Column(None, db.ForeignKey("entity_type.id"), nullable=False)
    mappings = db.relationship("EntityIntegerAttributeMapping",
                               backref="attribute")

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "type_id"])


class EntityFloatAttribute(db.Model):
    """Entity Attribute"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    type_id = db.Column(None, db.ForeignKey("entity_type.id"), nullable=False)
    mappings = db.relationship("EntityFloatAttributeMapping",
                               backref="attribute")

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "type_id"])


class EntityStringAttribute(db.Model):
    """Entity Attribute"""
    id = id_column()
    name = db.Column(db.String(64), unique=True, nullable=False)
    type_id = db.Column(None, db.ForeignKey("entity_type.id"), nullable=False)
    mappings = db.relationship("EntityStringAttributeMapping",
                               backref="attribute")

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "type_id"])


class Entity(db.Model):
    """Entity"""
    id = id_column()
    type_id = db.Column(None, db.ForeignKey("entity_type.id"), nullable=False)
    names = db.relationship("EntityNameMapping", backref="entity")
    integers = db.relationship("EntityIntegerAttributeMapping",
                               backref="entity")
    floats = db.relationship("EntityFloatAttributeMapping", backref="entity")
    strings = db.relationship("EntityStringAttributeMapping", backref="entity")
    relationships1 = db.relationship(
        "EntityRelationshipMapping",
        foreign_keys="EntityRelationshipMapping.entity1_id",
        backref="entity1")
    relationships2 = db.relationship(
        "EntityRelationshipMapping",
        foreign_keys="EntityRelationshipMapping.entity2_id",
        backref="entity2")

    def json(self):
        """JSON"""
        return to_dict(self, ["name", "type_id"])


class EntityNameMapping(db.Model):
    """Entity-Name Mapping"""
    id = id_column()
    entity_id = db.Column(None, db.ForeignKey("entity.id"), nullable=False)
    name_id = db.Column(None, db.ForeignKey("entity_name.id"), nullable=False)
    start_time = db.Column(db.Integer)
    end_time = db.Column(db.Integer)

    def json(self):
        """JSON"""
        return to_dict(self, ["entity_id", "name_id", "start_time",
                              "end_time"])


class EntityIntegerAttributeMapping(db.Model):
    """Entity-Attribute Mapping"""
    id = id_column()
    entity_id = db.Column(None, db.ForeignKey("entity.id"), nullable=False)
    attribute_id = db.Column(None,
                             db.ForeignKey("entity_integer_attribute.id"),
                             nullable=False)
    value = db.Column(db.Integer)
    weight = db.Column(db.Integer, default=1)
    start_time = db.Column(db.Integer)
    end_time = db.Column(db.Integer)

    def json(self):
        """JSON"""
        return to_dict(self, ["entity_id", "attribute_id", "value", "weight",
                              "start_time", "end_time"])


class EntityFloatAttributeMapping(db.Model):
    """Entity-Attribute Mapping"""
    id = id_column()
    entity_id = db.Column(None, db.ForeignKey("entity.id"), nullable=False)
    attribute_id = db.Column(None,
                             db.ForeignKey("entity_float_attribute.id"),
                             nullable=False)
    value = db.Column(db.Float)
    weight = db.Column(db.Integer, default=1)
    start_time = db.Column(db.Integer)
    end_time = db.Column(db.Integer)

    def json(self):
        """JSON"""
        return to_dict(self, ["entity_id", "attribute_id", "value", "weight",
                              "start_time", "end_time"])


class EntityStringAttributeMapping(db.Model):
    """Entity-Attribute Mapping"""
    id = id_column()
    entity_id = db.Column(None, db.ForeignKey("entity.id"), nullable=False)
    attribute_id = db.Column(None,
                             db.ForeignKey("entity_string_attribute.id"),
                             nullable=False)
    value = db.Column(db.String(2048))
    weight = db.Column(db.Integer, default=1)
    start_time = db.Column(db.Integer)
    end_time = db.Column(db.Integer)

    def json(self):
        """JSON"""
        return to_dict(self, ["entity_id", "attribute_id", "value", "weight",
                              "start_time", "end_time"])


class EntityRelationshipMapping(db.Model):
    """Entity-Relationship Mapping"""
    id = id_column()
    relationship_id = db.Column(None,
                                db.ForeignKey("entity_relationship.id"),
                                nullable=False)
    entity1_id = db.Column(None, db.ForeignKey("entity.id"), nullable=False)
    entity2_id = db.Column(None, db.ForeignKey("entity.id"), nullable=False)
    weight = db.Column(db.Integer, default=1)
    start_time = db.Column(db.Integer)
    end_time = db.Column(db.Integer)

    def json(self):
        """JSON"""
        return to_dict(self, ["relationship_id", "entity1_id", "entity2_id",
                              "weight", "start_time", "end_time"])

# Endpoints


class BusinessResource(QueryResource):
    def ingest(self, item):
        obj = get_or_create(self.query_class, id=item["id"])
        for key, value in item.items():
            if key == "id":
                continue

            key_id = "%s_id" % key
            if hasattr(obj, key_id):
                setattr(obj, key_id, value)
            else:
                setattr(obj, key, value)

    @require_auth
    def put(self):
        try:
            for item in request.get_json(force=True):
                self.ingest(item)
            commit()
            return "", 204
        except Exception as e:
            rollback()
            return str(e), 400

    @require_auth
    def delete(self):
        for item in self.get_raw():
            delete(item)
        commit()
        return "", 204


class EntityTypeResource(BusinessResource):
    query_class = EntityType


class EntityNameResource(BusinessResource):
    query_class = EntityName


class EntityRelationshipResource(BusinessResource):
    query_class = EntityRelationship


class EntityIntegerAttributeResource(BusinessResource):
    query_class = EntityIntegerAttribute


class EntityFloatAttributeResource(BusinessResource):
    query_class = EntityFloatAttribute


class EntityStringAttributeResource(BusinessResource):
    query_class = EntityStringAttribute


class EntityResource(BusinessResource):
    query_class = Entity


class EntityNameMappingResource(BusinessResource):
    query_class = EntityNameMapping


class EntityIntegerAttributeMappingResource(BusinessResource):
    query_class = EntityIntegerAttributeMapping


class EntityFloatAttributeMappingResource(BusinessResource):
    query_class = EntityFloatAttributeMapping


class EntityStringAttributeMappingResource(BusinessResource):
    query_class = EntityStringAttributeMapping


class EntityRelationshipMappingResource(BusinessResource):
    query_class = EntityRelationshipMapping


def setup():
    """Let's roll."""

    resources = {
        "/entity": EntityResource,
        "/entity/type": EntityTypeResource,
        "/entity/name": EntityNameResource,
        "/entity/relationship": EntityRelationshipResource,
        "/attribute/integer": EntityIntegerAttributeResource,
        "/attribute/float": EntityFloatAttributeResource,
        "/attribute/string": EntityStringAttributeResource,
        "/mapping/name": EntityNameMappingResource,
        "/mapping/relationship": EntityRelationshipMappingResource,
        "/mapping/attribute/integer": EntityIntegerAttributeMappingResource,
        "/mapping/attribute/float": EntityFloatAttributeMappingResource,
        "/mapping/attribute/string": EntityStringAttributeMappingResource
    }

    configure(resources)


setup()

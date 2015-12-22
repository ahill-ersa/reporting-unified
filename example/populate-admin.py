#!/usr/bin/env python3

import json
import uuid

import requests

base = "http://localhost:8000"
token = "foo"

put_headers = {"content-type": "application/json", "x-ersa-auth-token": token}
get_headers = {"x-ersa-auth-token": token}


def generate(endpoint, **kwargs):
    item = kwargs
    item["id"] = str(uuid.uuid4())
    return {"item": item,
            "response": requests.put("%s/%s" % (base, endpoint),
                                     headers=put_headers,
                                     data=json.dumps([item]))}


def query(endpoint, filters):
    query = "&".join([("filter=%s" % f) for f in filters])
    return requests.get("%s/%s?%s" % (base, endpoint, query),
                        headers=get_headers).json()


def equality_query(endpoint, **kwargs):
    filters = ["%s.eq.%s" % (key, value) for (key, value) in kwargs.items()]
    return query(endpoint, filters)


generate("entity/type", name="organisation")
generate("entity/type", name="person")
generate("entity/type", name="service")

organisation = equality_query("entity/type", name="organisation")[0]
person = equality_query("entity/type", name="person")[0]
service = equality_query("entity/type", name="service")[0]

generate("entity/relationship",
         name="member",
         valid1=person["id"],
         valid2=organisation["id"])
generate("entity/relationship",
         name="unit",
         valid1=organisation["id"],
         valid2=organisation["id"])
generate("entity/relationship",
         name="user",
         valid1=person["id"],
         valid2=service["id"])

member = equality_query("entity/relationship", name="member")[0]
unit = equality_query("entity/relationship", name="unit")[0]
user = equality_query("entity/relationship", name="user")[0]

generate("entity/name", name="ACME University")

acme_name = equality_query("entity/name", name="ACME University")[0]

acme = generate("entity", type=organisation["id"])["item"]

print(acme_name)
print(acme)

acme_name_mapping = generate("mapping/name",
                             entity=acme["id"],
                             name=acme_name["id"],
                             start_time=0)["item"]

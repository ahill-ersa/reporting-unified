""" A flask application for generating bills """


import json
import logging
import csv
import os.path

import click
from flask.cli import FlaskGroup

from ersa_reporting import app

app.config.from_envvar('APP_SETTINGS')

# flask-sqlalchemy issue: multiple databases shared the same MetaData object
# https://github.com/mitsuhiko/flask-sqlalchemy/pull/222
# Until this has been released in v3, do manual patch
# wget https://raw.githubusercontent.com/ryanss/flask-sqlalchemy/8b5c6c7cbc3c33db27ed2f6d505ad56863e404e8/flask_sqlalchemy/__init__.py

# Only load these packages when app has been configured
# from ersa_reporting.models import nova, keystone
from ersa_reporting.models.nova import Summary, Instance, Flavor  # noqa: E402
from ersa_reporting.models.keystone import Tenant  # noqa: E402
from nectar import Keystone  # noqa: E402

# logging.basicConfig(format='%(module)s - %(name)s %(asctime)s %(levelname)s: %(message)s', level=logging.DEBUG)
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.DEBUG)
logger = logging.getLogger()
# Stop upstream libraries polluting our debug logger
logging.getLogger("keystoneauth").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)


# Based on https://wiki.python.org/moin/SortingListsOfDictionaries and others
def multikeysort(items, columns):
    from operator import itemgetter
    from functools import cmp_to_key

    comparers = [((itemgetter(col[1:].strip()), -1) if col.startswith('-') else (itemgetter(col.strip()), 1)) for col in columns]

    # recreate cmp from python2
    def cmp(a, b):
        return (a > b) - (a < b)

    def comparer(left, right):
        for fn, mult in comparers:
            result = cmp(fn(left), fn(right))
            if result:
                return mult * result
        else:
            return 0
    return sorted(items, key=cmp_to_key(comparer))


def print_json(jobj):
    print(json.dumps(jobj, indent=2))


def array_to_dict(rows, key="openstack_id"):
    """ Convert a list of dicts into a dict based on key"""
    result = {}
    for row in rows:
        key_value = row.pop(key)
        result[key_value] = row

    return result


def repack(dict_obj, key_map, rm_keys=[]):
    """ Repackage a dict object by renaming and removing keys"""
    for k, v in key_map.items():
        dict_obj[v] = dict_obj.pop(k)
    for k in rm_keys:
        del dict_obj[k]

    return dict_obj


def get_flavors():
    """ Get Nectar vm flavors in a dict with openstack_id as key """
    fls = Flavor.query.all()
    results = []
    for fl in fls:
        results.append(repack(fl.json(), {"name": "flavor_name"}, ["id"]))

    return array_to_dict(results)


def get_keystoneclient():
    return Keystone(app.config["NECTAR_USER"], app.config["NECTAR_USER_PASS"], 'Admin')


def get_tenants(domain):
    """ Get tenants of a domain in a dict with openstack_id as key """
    logger.debug("Getting teants in %s" % domain)
    tenants = Tenant.in_domain(domain)
    for tenant in tenants:
        logger.debug(tenant)

    logger.debug("Getting managers from Keystone")
    results = []
    kclient = get_keystoneclient()
    for tenant in tenants:
        tenant["manager"] = ""
        tenant["email"] = ""
        # only get the first manager of non-personal tenant
        # personal tenant has not manager
        managers = kclient.get_tenant_manangers(tenant["openstack_id"])
        if len(managers) > 0:
            if len(managers) > 1:
                logger.warn("More than one manager found, only one set")
                print(managers)
            manager = managers[0]
            tenant["manager"] = manager["name"]
            tenant["email"] = manager["email"]
        results.append(repack(tenant, {"name": "tenant_name"}))

    return array_to_dict(results, "openstack_id")


class InstanceScore(object):
    """ Translate a nova instace flavor to a charge score """

    def __init__(self):
        self.flavors = get_flavors()

    def _raw(self, flavor, attr="vcpus"):
        """ return raw number as score """
        return self.flavors[flavor][attr]

    def get_scrore(self, flavor):
        # Currently, use raw vcpus as score
        return self._raw(flavor)


def summarise_tenant(instance_states):
    """ Summaries usage of Nova tenant """
    core_usages = {}
    calculator = InstanceScore()

    for state in instance_states:
        if state["tenant"] not in core_usages:
            core_usages[state["tenant"]] = 0
        core_usages[state["tenant"]] += calculator.get_scrore(state["flavor"])

    return core_usages


def _to_list(obj, fields):
    result = []
    for field in fields:
        result.append(obj[field])
    return result


def generate_csv(file_name, data, fields):
    with open(file_name, "w", newline="") as csvfile:
        spamwriter = csv.writer(csvfile)
        spamwriter.writerow([field.capitalize() for field in fields])
        for line in data:
            print(line)
            spamwriter.writerow(_to_list(line, fields))


@app.cli.command()
@click.argument("start", type=int)
@click.argument("end", type=int)
@click.argument("domain")
def nova(start, end, domain):
    """
    Get a summary of instances on sa node.
    START, END: time stamps of start and end dates, DOMAIN: domain string
    """
    logger.info("Query arguments: start=%s, end=%s, domain=%s" % (start, end, domain))
    q = Summary(start, end)
    ids = q.value()
    logger.debug("Total number of instances = %d" % len(ids))

    if len(ids) > 0:
        logger.debug("Getting tenants of %s" % domain)
        tenants = get_tenants(domain)
        logger.debug("Found %d tenants" % len(tenants))

        if len(tenants) == 0:
            logger.error("No tenant in %s has been found" % domain)
            return

        logger.debug("Filter out instances from a base of %d" % len(ids))
        states = []
        for id in ids:
            logger.debug(id)
            instance = Instance.query.get(id)
            logger.debug("Getting its latest state")
            state = instance.latest_state(start, end)

            if state["tenant"] in tenants:
                states.append(state)

        logger.info("%d instances found for %s" % (len(states), domain))

        summaries = summarise_tenant(states)
        results = []
        for tenant in summaries:
            summary = {"tenant": tenants[tenant]["tenant_name"],
                       "score": summaries[tenant]}
            summary.update(tenants[tenant])
            results.append(summary)
        print_json(results)

        csv_name = "%s_nova_%s-%s.csv" % (domain, start, end)
        generate_csv(csv_name, results, ["tenant", "manager", "email", "score"])


def create_nova_app(info):
    return app

def _replace_ext(ori_path, ext):
    if ext[0] != '.':
        ext = '.' + ext

    return os.path.join(
        os.path.dirname(ori_path),
        os.path.basename(ori_path).split('.')[0] + ext)

# curl -H x-auth-token:very_long_nectar_token https://reporting-api.rc.nectar.org.au:9494/v1/reports/project > all_project.json
# Only public and has instance: reports/project?personal=0&has_instances=1
# A particular one: reports/project?id=6119cd97a3bd478d809845aeacc6ea12
def convert_nectar_json(json_file_name):
    # Get all projects from nectar reporting api.
    # It has these fields:
    # fields = ['personal', 'quota_volume_count', 'enabled', 'quota_vcpus', 'quota_memory', 'id', 'description', 'quota_snapshot', 'quota_volume_total', 'organisation', 'has_instances', 'display_name', 'quota_instances']
    with open(json_file_name) as json_file:
        data = json.load(json_file)

    if len(data) == 0:
        return

    csv_file_name = os.path.join(
        os.path.dirname(json_file_name),
        os.path.basename(json_file_name).split('.')[0] + '.csv')

    fields = list(data[0].keys())
    with open(csv_file_name, 'w') as writer:
        csv_writer = csv.writer(writer)
        csv_writer.writerow(fields)
        for d in data:
            csv_writer.writerow(list(d.values()))

def csv_to_json(csv_file_name):
    # Convert cleand csv file to json for frontend to consume when there is no
    # database table or no need of it

    rows = []
    with open(csv_file_name, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            openstack_id = row.pop('id')
            rows.append({openstack_id: row})

    if len(rows) == 0:
        logger.info('No row read from %s' % csv_file)
        return

    json_file_name = _replace_ext(csv_file_name, 'json')
    with open(json_file_name, 'w') as json_file:
        json.dump(rows, json_file)

def test_in_domain(domain):
    tenants = Tenant.in_domain(domain)
    for tenant in tenants:
        logger.debug(tenant)


@app.cli.command()
def test():
    #f = get_tenants('unisa.edu.au')
    #f = get_tenants('adelaide.edu.au')
    test_in_domain('adelaide.edu.au')
    #f = get_flavors()
    #print_json(f)

    # cal = InstanceScore()
    # print(cal.get_scrore("1"))

    #~ kclient = Keystone(app.config["NECTAR_USER"], app.config["NECTAR_USER_PASS"], 'Admin')
    #~ for prj in f:
        #~ print(f[prj]['tenant_name'], f[prj]['allocation'])
        #~ managers = kclient.get_tenant_manangers(prj)
        #~ print(managers)
    logger.info("Completed")


# None personal project
@app.cli.command()
@click.argument("domain", default="unisa.edu.au")
def export_tenants(domain):
    logger.debug("Getting teants in %s" % domain)
    tenants = Tenant.in_domain(domain)
    results = []

    logger.debug("Getting managers from Keystone")
    kclient = get_keystoneclient()
    file_name = "%s_tenants.csv" % domain
    with open(file_name, "w", newline="") as csvfile:
        spamwriter = csv.writer(csvfile)
        for tenant in tenants:
            if not tenant['name'].startswith('pt-'):
                fields = _to_list(tenant, ["openstack_id", "allocation", "name", "description"])
                managers = kclient.get_tenant_manangers(tenant["openstack_id"])
                for manager in managers:
                    fields.extend(_to_list(manager, ["id", "name", "email", "domain"]))
                spamwriter.writerow(fields)
            else:
                print(tenant['name'])

# Convert json file downloaded from Nectar reporting site:
# https://reporting-api.rc.nectar.org.au:9494/v1/reports/project?personal=0&has_instances=1
@app.cli.command()
@click.argument("file_name", default='all_projects.json')
def convert_to_csv(file_name):
    convert_nectar_json(file_name)

# Convert a csv to json with openstack_id as key for frontend app
@app.cli.command()
@click.argument("file_name", default='all_projects.csv')
def convert_to_json(file_name):
    csv_to_json(file_name)

@click.group(cls=FlaskGroup, add_default_commands=False, create_app=create_nova_app)
def cli():
    """This is a management script for the wiki application."""
    # Put common routines before executing each command
    logger.info("Start a command")


if __name__ == '__main__':
    cli()

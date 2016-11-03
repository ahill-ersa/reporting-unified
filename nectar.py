""" A temporay library to help eRSA reporting until Nectar reporting APIs are availabe
"""

from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneclient.v3 import client
from novaclient import client as nova_client


AUTH_URL = 'https://keystone.rc.nectar.org.au:5000/v3'
NOVA_VERSION = 2


def to_dict(object, attrs):
    """Generate dictionary with specified attributes."""
    output = {}

    for attr in attrs:
        if hasattr(object, attr):
            if ":" in attr:
                # to remove the part before the colon: e.g. OS-FLV-EXT-DATA:ephemeral
                # to match the output of command
                short_attr = attr.split(":")[1]
                output[short_attr] = getattr(object, attr)
            else:
                output[attr] = getattr(object, attr)
    return output


def create_session(username, password, project_name):
    auth = v3.Password(auth_url=AUTH_URL,
                       username=username,
                       password=password,
                       project_name=project_name,
                       project_domain_name='default',
                       user_domain_name='default')

    return session.Session(auth=auth)


def get_domain(name):
    """Extract an organisational domain from an email address.
       For Australian educational institutions, last 3 parts.
       For US educational institutions, last 2 parts.
       For the rest, domain name of email address.
    """
    if "@" in name:
        domain_name = name.split("@")[1]
        if domain_name.endswith(".edu.au"):
            domain_name = ".".join(domain_name.split(".")[-3:])
        elif domain_name.endswith(".edu"):
            domain_name = ".".join(domain_name.split(".")[-2:])
        return domain_name
    else:
        return None


class Keystone(object):
    # id: openstack_id in models
    USER_ATTRS = ["default_project_id", "email", "name", "id"]
    TENANT_ATTRS = ["allocation_id", "name", "description", "enabled", "id"]
    # Nectar also has domain_id, expires, status, parent_id

    def __init__(self, username, password, project_name):
        sess = create_session(username, password, project_name)
        self.client = client.Client(session=sess)

        roles = self.client.roles.list()
        self.role_dict = {role.name: role.id for role in roles}

    def get_user(self, user_id):
        """Get a user information"""
        meta = to_dict(self.client.users.get(user_id), self.USER_ATTRS)
        if 'email' in meta and meta['email'] and len(meta['email'].strip()):
            meta['domain'] = get_domain(meta['email'].strip())
        else:
            meta['domain'] = ''
        return meta

    def get_role_id(self, name="TenantManager"):
        # TenantManager 14
        # Member 2
        return self.role_dict[name]

    # Here tenant == project
    def get_tenants(self):
        projects = self.client.projects.list()
        return [to_dict(project, self.TENANT_ATTRS) for project in projects]

    def get_tenant(self, project_id):
        """Get a tenant(project) information"""
        return to_dict(self.client.projects.get(project_id), self.TENANT_ATTRS)

    def get_managers(self, project_id=None):
        """Get a list of roles"""
        manager_role_id = self.get_role_id()
        assignments = self.client.role_assignments.list(role=manager_role_id)

        managers = {}
        for assignment in assignments:
            project_id = assignment.scope['project']['id']
            user = self.get_user(assignment.user['id'])
            del user['default_project_id']
            if project_id in managers:
                managers[assignment.scope['project']['id']].append(user)
            else:
                managers[assignment.scope['project']['id']] = [user]
        return managers

    def _get_tenant_mananger_ids(self, project_id):
        """Get id of managers of a tenant(project)"""
        manager_role = self.get_role_id()
        assignments = self.client.role_assignments.list(project=project_id, role=manager_role)
        manager_ids = [assignment.user["id"] for assignment in assignments]
        return manager_ids

    def get_tenant_manangers(self, project_id):
        manager_ids = self._get_tenant_mananger_ids(project_id)
        managers = []
        for manager_id in manager_ids:
            managers.append(self.get_user(manager_id))
        return managers

    def get_tenant_domains(self, project_id):
        """Get domain (guessed from manager's email) of a tenant(project)
        """
        managers = self.get_tenant_manangers(project_id)
        return [manager['domain'] for manager in managers]


class Nova(object):
    FLAVOR_ATTRS = ["id", "name", "ram", "vcpus", "disk", "swap", "rxtx_factor",
                    "OS-FLV-EXT-DATA:ephemeral", "OS-FLV-DISABLED:disabled"]

    def __init__(self, username, password, project_name):
        sess = create_session(username, password, project_name)
        self.client = nova_client.Client(NOVA_VERSION, session=sess)

    def get_flavors(self):
        """Get instance flavors in a list of dicts"""
        flavor_objs = self.client.flavors.list()
        flavors = []
        for flavor in flavor_objs:
            flavors.append(to_dict(flavor, self.FLAVOR_ATTRS))
        return flavors

    def get_quota(self, project_id):
        """Get quota dict of a project"""
        quotas = self.client.quotas.get(project_id)
        return quotas.to_dict()


if __name__ == '__main__':
    from argparse import ArgumentParser
    import random

    parser = ArgumentParser(description="Demo script of queries on Nectar Keystone")

    parser.add_argument('-u', '--username', required=True)
    parser.add_argument('-p', '--password', required=True)
    args = parser.parse_args()

    nclient = Nova(args.username, args.password, 'Admin')
    print(nclient.get_flavors())
    print(nclient.get_quota('6119cd97a3bd478d809845aeacc6ea12'))
    exit(0)

    kclient = Keystone(args.username, args.password, 'Admin')
    # test_get_tenant(kclient, '8d6f8f0aa02048fbb2fbd7daad94fd61')
    # test_get_tenant(kclient, 'c00ec4654f9d4f1da70b63d8649c6718')
    # test_get_user(kclient, 'e001f58e113c41c3a3a153846bab69a3')
    # exit(0)
    results = kclient.get_managers()
    print(results)
    import json
    with open('nectar_tenant_managers.json', 'w') as jf:
        json.dump(results, jf)
    exit(0)

    tenants = kclient.get_tenants()
    selected = random.sample(range(len(tenants)), 5)
    print("You may see managers if a tenant is not a personal tenant.\n")
    for select in selected:
    # for select in range(len(tenants)):
        print(tenants[select])
        managers = kclient.get_tenant_manangers(tenants[select]['id'])
        for manager in managers:
            print(manager)

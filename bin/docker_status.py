#!/home/ubuntu/docker_status/bin/python

"""
Docker monitoring tool

Author: Andrew Hill (andrew.hill@ersa.edu.au)
"""

import slackweb
from docker import Client
from pprint import pprint

# Initialise the slack API client
slack = slackweb.Slack(url="<redacted>")

# Initialise slack message
message = []

# Check these reporting packages
reporting_packages = ["swift", "keystone", "hpc", "hnas", "hcp", "nova", "xfs"]

# Create dict
reporting_packages_dict = {}

# Fill dict
for package in reporting_packages:
    reporting_packages_dict[package] = "Not found"

# Version of the server API for docker
docker_server_api_version = '1.20'

# Connect to the docker API
api = Client(base_url='unix://var/run/docker.sock',version=docker_server_api_version)

# List of status a docker container can be in
filter_list = ["created", "restarting", "running", "paused", "exited", "dead"]

# Now go through each status
for filt in filter_list:

    # Generate a containers dict
    containers = api.containers(filters={"status":filt})

    # Now let's check the status of each container
    for container in containers:
        label = container["Labels"]["reporting/package"]
        #status = container["Status"]
        #print("label[{}] status[{}]".format(label, status))
        if label in reporting_packages:
            reporting_packages_dict[label] = filt

for package in reporting_packages:
    status = reporting_packages_dict[package]
    if status == "running":
        message.append(":white_check_mark: {} ({})".format(package, status))
    else:
        message.append(":x: {} ({})".format(package, status))

# Send the message to slack
if message:
    slack.notify(text="\n".join(message))

#!/data/p2/bin/python2

"""
ersa-kr monitoring tool

Author: Andrew Hill (andrew.hill@ersa.edu.au)
"""

import os
import slackweb
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import arrow
import ersa_reporting_kafka
import ersa_reporting_kafka.api as api

# Disable the many self signed warnings generated while running this script
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Enter the slack token URL here
slack = slackweb.Slack(url="<redacted>")

# Allow up to 2 hours before reporting there may be an issue
grace_seconds = 7200

# Topics to ignore
ignore_topics = ["nectar.rabbitmq", "nectar.swift", "test7", "nectar.xfs"]

#setup environment variables. https_verify set to False is the equivalent of "--insecure" on the command line
api_config = {
    "server": "<redacted>",
    "username": "<redacted>",
    "token": "<redacted>",
    "https_verify": False
}

# Get the status from the API
status = api.API(**api_config).list()

# Initialise the array that will contain the Slack message
message = []

# Sort the dict alphabetically into an array
topics = sorted(list(status.keys()))

# Get the current UTC time
now_ts = arrow.utcnow().timestamp

# Go through each topic
for topic in topics:
    if not topic in ignore_topics:
        # Get the metadata from the topic
        metadata = status[topic]
        try:
            last_ts = metadata["latest_timestamp"].timestamp
            last_human = metadata["latest_timestamp"].humanize()
            # Calculate the difference between the "now" time above and the last message time from the topic.
            difference = now_ts - last_ts
            # Now make sure that is under the grace_seconds
            if difference < grace_seconds:
                # Everything is fine
                message.append(":white_check_mark: {} ({})".format(topic, last_human))
            else:
                # Uh-oh, something has broken
                message.append(":x: {} ({})".format(topic, last_human))
        except AttributeError:
            message.append(":x: {} (unable to retrieve timestamp)".format(topic))

# Send the message to slack
if message:
    slack.notify(text="\n".join(message))

#!/home/ubuntu/ersa-reporting-hcp-bot/bin/python

"""
HCP monitoring tool

Author: Andrew Hill (andrew.hill@ersa.edu.au)
"""

import base64
import hashlib
import ssl
from boto.s3.connection import S3Connection
import requests
from datetime import datetime
import arrow
from concurrent import futures
from pprint import pprint
import sys
import slackweb

requests.packages.urllib3.disable_warnings()

archive_prefix = "20160113-112448/"
grace_seconds = 86400

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

slack = slackweb.Slack(url="<redacted>")

username = "<redacted>"
password = "<redacted>"

aws_id = base64.b64encode(username)
aws_secret = hashlib.md5(password).hexdigest()

host = "reporting.hcp1.s3.ersa.edu.au"

s3 = S3Connection(aws_access_key_id=aws_id, aws_secret_access_key=aws_secret, host=host)

message = []
archive = s3.get_bucket("archive")
topics = []
topics_skip = ["emu.linux","emu.nfs","emu.pbs","nectar.libvirt","nectar.linux","nectar.mysql","nectar.rabbitmq","nectar.swift","nectar.xfs","nectar.zfs","storage.linux","tizard.linux","tizard.nfs"]
topics_dict = {}
topics_skip_prefix = map(lambda t: archive_prefix + t + "/", topics_skip)

def check_partition(partition_name):
    objects = {}
    json_list = archive.list(prefix=partition_name, delimiter=".json.xz")
    for json_last_file in json_list:
       json_file = archive.get_key(json_last_file.name)
       timestamp = datetime.strptime(json_file.last_modified, "%a, %d %b %Y %H:%M:%S GMT")
       objects[json_last_file.name.encode("ascii")] = timestamp

    last_file = max(objects, key=lambda key: objects[key])
    arrow_obj = arrow.get(objects[last_file])
    topic_name = last_file.split("/")[1]
    topic_partition = last_file.split("/")[2]
    topic_file = last_file.split("/")[3]

    retdict = {"name":topic_file, "arrow_obj":arrow_obj, "partition": topic_partition}

    return retdict

def check_topic(topic_name):
    partitions_raw = map(lambda p: p.name.encode("ascii"), archive.list(prefix=topic_name, delimiter="/"))
    partitions = filter(lambda n: n != topic, partitions_raw)
    with futures.ThreadPoolExecutor(max_workers=8) as executor:
        for partition_info in executor.map(check_partition, partitions):
            topic_partition = partition_info["partition"]
            topic_file = partition_info["name"]
            arrow_obj = partition_info["arrow_obj"]
            topics_dict[topic][topic_partition] = {"name":topic_file, "arrow_obj":arrow_obj}

for topic in archive.list(prefix=archive_prefix, delimiter="/"):
    topic_name = topic.name.encode("ascii")
    #if topic_name != archive_prefix:
    if topic_name != archive_prefix and not topic_name in topics_skip_prefix:
        #print "name:[{}]".format(topic_name)
        topics_dict[topic_name] = {}
        topics.append(topic_name)

for topic in topics:
    partitions_raw = map(lambda p: p.name.encode("ascii"), archive.list(prefix=topic, delimiter="/"))
    partitions = filter(lambda n: n != topic, partitions_raw)
    with futures.ProcessPoolExecutor(max_workers=8) as executor:
        for partition_info in executor.map(check_partition, partitions):
            topic_partition = partition_info["partition"]
            topic_file = partition_info["name"]
            arrow_obj = partition_info["arrow_obj"]
            topics_dict[topic][topic_partition] = {"name":topic_file, "arrow_obj":arrow_obj}
    times = map(lambda o: o["arrow_obj"].timestamp, topics_dict[topic].values())
    time_now = arrow.utcnow()
    time_diff = map(lambda t: time_now.timestamp - t, times)
    newest_seconds = min(time_diff)
    newest = time_now.replace(seconds=-newest_seconds)
    topics_dict[topic]["newest"] = newest
    topics_dict[topic]["newest_seconds"] = newest_seconds
    topic_name = topic.split("/")[1]
    if newest_seconds > grace_seconds:
        message.append(":x: {} (newest was: {})".format(topic_name, newest.humanize()))
    else:
        message.append(":white_check_mark: {} (newest was: {})".format(topic_name, newest.humanize()))
                
# Send the message to slack
if message:
    slack.notify(text="\n".join(message))

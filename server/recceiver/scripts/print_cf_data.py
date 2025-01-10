from channelfinder import ChannelFinderClient
import json
import os
from operator import itemgetter

filename = "/home/devuser/cfdata"  # change this to output file name
client = ChannelFinderClient()


def get_cf_data(client):
    channels = client.findByArgs([("pvStatus", "Active")])

    for ch in channels:
        ch.pop("owner", None)
        ch.pop("tags", None)
        for prop in ch["properties"]:
            if prop["name"] == "hostName":
                ch["hostName"] = prop["value"]
            if prop["name"] == "iocName":
                ch["iocName"] = prop["value"]
        ch.pop("properties", None)
    return channels


channels = get_cf_data(client)

if os.path.isfile(filename):
    os.remove(filename)

new_list = []

for channel in channels:
    new_list.append([channel["name"], channel["hostName"], int(channel["iocName"])])

new_list.sort(key=itemgetter(0))

with open(filename, "wrx") as f:
    json.dump(new_list, f)

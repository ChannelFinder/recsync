import json
import os
from operator import itemgetter

from channelfinder import ChannelFinderClient

filename = "/home/devuser/cfdata"  # change this to output file name
client = ChannelFinderClient()


def get_cf_data(client):
    channels = client.findByArgs([("pvStatus", "Active")])

    for cf_channel in channels:
        cf_channel.pop("owner", None)
        cf_channel.pop("tags", None)
        for cf_property in cf_channel["properties"]:
            if cf_property["name"] == "hostName":
                cf_channel["hostName"] = cf_property["value"]
            if cf_property["name"] == "iocName":
                cf_channel["iocName"] = cf_property["value"]
        cf_channel.pop("properties", None)
    return channels


channels = get_cf_data(client)

if os.path.isfile(filename):
    os.remove(filename)

new_list = []

for channel in channels:
    new_list.append([channel["name"], channel["hostName"], int(channel["iocName"])])

new_list.sort(key=itemgetter(0))

with open(filename, "x") as f:
    json.dump(new_list, f)

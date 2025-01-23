from channelfinder import ChannelFinderClient

"""
Simple script for adding active channels to Channel Finder Service for testing cf-store clean
If it gives a 500 error, run it again. Glassfish and CFS must be set up and running.
"""


def abbr(name, hostname, iocname, status):
    return {
        "owner": "cf-update",
        "name": name,
        "properties": [
            {"owner": "cf-update", "name": "hostName", "value": hostname},
            {"owner": "cf-update", "name": "iocName", "value": iocname},
            {"owner": "cf-update", "name": "pvStatus", "value": status},
            {
                "owner": "cf-update",
                "name": "time",
                "value": "2016-08-18 12:33:09.953985",
            },
        ],
    }


client = ChannelFinderClient()
client.set(
    channels=[
        abbr("ch1", "testhosta", 1111, "Active"),
        abbr("test_channel", "testhosta", 1111, "Active"),
    ]
)

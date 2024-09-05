import json

import urllib3


def fake_channel(ch_name: str, recc: str, status: str):
    return {
        "name": ch_name,
        "owner": recc,
        "properties": [
            {"name": "recceiverID", "owner": recc, "value": recc},
            {"name": "pvStatus", "owner": recc, "value": status},
        ],
    }


def insert_fake_data(recc: str, cf: str, chs: list[str], status: str):
    http = urllib3.PoolManager()
    headers = urllib3.make_headers(basic_auth="admin:password")
    headers.update({"Content-Type": "application/json"})
    req = http.request(
        "PUT",
        cf + "/ChannelFinder/resources/channels",
        headers=headers,
        body=json.dumps([fake_channel(ch, recc, status) for ch in chs]),
    )


cf = "http://localhost:8080"

insert_fake_data("recc1", cf, ["ch_1_" + str(i) for i in range(10002)], "Active")
insert_fake_data("recc2", cf, ["ch_2_" + str(i) for i in range(100)], "Active")

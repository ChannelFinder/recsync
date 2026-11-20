# See files LICENSE and COPYRIGHT
# SPDX-License-Identifier: EPICS

from configparser import ConfigParser

__all__ = (
    'loadConfig',
)

def loadConfig(fname : str) -> ConfigParser:
    P = ConfigParser()

    P['recceiver'] = {
        'announceInterval': '30.0',
        'tcptimeout': '15.0',
        'commitInterval': '5.0',
        'commitSizeLimit': str(16*1024),
        'maxActive': '20',
        'addrlist': '',
        'bind': '0.0.0.0:0',
    }

    with open(fname, 'r') as F:
        P.read(F)

def parse_ep(s, *, defport=0):
    addr, _sep, port = s.partition(':')

    return (addr, int(port or defport))

# Recceiver

Application for talking between IOCs (via [reccaster](../client)) and ChannelFinder (via [pyCFClient](https://github.com/ChannelFinder/pyCFClient)).

Written using [twistd](https://twisted.org/).


## Server testing

Setup

```bash
 sqlite3 test.db -init recceiver.sqlite3 .exit
```

Run (for twistd <= 16.0.3)

```bash
twistd -n recceiver -f demo.conf
```

or (see below for discussion)

```bash
twistd -r poll -n recceiver -f demo.conf
```


Run (for twistd >= 16.0.4)

```bash
PYTHONPATH=$PWD twistd -r poll -n recceiver -f demo.conf
```

At some point 'twistd' stopped implicitly searching the working directory.

May need to uncomment `addrlist = 127.255.255.255:5049` in demo.conf
when doing local testing on a computer w/ a firewall.

Twisted 14.0.2 seems to have a problem with the epoll() reactor
which raises 'IOError: [Errno 2] No such file or directory'
during startup.  Try with the poll() reactor.

```bash
twistd -r poll -n recceiver -f demo.conf
```

# Recceiver

Application for talking between IOCs (via [reccaster](../client)) and ChannelFinder (via [pyCFClient](https://github.com/ChannelFinder/pyCFClient)).

Written using [twistd](https://twisted.org/).

## Docker

There is an example docker compose script which runs recceiver and channelfinder together.

```bash
docker compose up
```

## Formatting and Linting

Recceiver uses [ruff](https://docs.astral.sh/ruff/) for formatting and linting. See website for installation instructions.


```bash
ruff check
```

```bash
ruff check --fix
```

```bash
ruff format
```


## Server testing

Setup

```bash
 sqlite3 test.db -init recceiver.sqlite3 .exit
```

Run (for twistd >= 16.0.4)

```bash
PYTHONPATH=$PWD twistd --nodaemon recceiver -f demo.conf
```

At some point 'twistd' stopped implicitly searching the working directory.

May need to uncomment `addrlist = 127.255.255.255:5049` in demo.conf
when doing local testing on a computer w/ a firewall.

### Older Recceiver/Twistd Versions

For recceiver <= 1.6, passing the poll reactor was required. See [here](https://github.com/ChannelFinder/recsync/issues/132) for more discussion.

```bash
PYTHONPATH=$PWD twistd -r poll --nodaemon recceiver -f demo.conf
```

For older versions of twistd (<= 16.0.3), run

```bash
twistd --nodaemon recceiver -f demo.conf
```

or (see below for discussion)

```bash
twistd -r poll --nodaemon recceiver -f demo.conf
```

Twisted 14.0.2 seems to have a problem with the epoll() reactor
which raises 'IOError: [Errno 2] No such file or directory'
during startup.  Try with the poll() reactor.

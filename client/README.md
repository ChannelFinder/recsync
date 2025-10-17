# Reccaster

Reccaster is an EPICS module to facilitate the communication between IOCs and [recceiver](../server). Before `iocInit`, reccaster will publish all records and their metadata to recceiver.

## IOC Shell Functions and Variables

See the demo [st.cmd](iocBoot/iocdemo/st.cmd) for examples of configuring each of these optional settings.

### addReccasterExcludePattern

addReccasterExcludePattern allows you to exclude PVs from being sent by reccaster via wildcard patterns. It must be called before `iocInit`.

You can specify multiple exclusion patterns:

```
addReccasterExcludePattern("*_")
addReccasterExcludePattern("*:Internal:*")
```

Adding this call to your st.cmd would disable all PVs from being sent to recceiver:

```
addReccasterExcludePattern("*")
```

### addReccasterEnvVars

addReccasterEnvVars allows you to specify extra environment variables to send along with each PV. It must be called before `iocInit`.

For example, you might define which building an IOC is in via `epicsEnvSet("BUILDING", "MyBuilding")`. You can then include this in reccaster by adding this in your st.cmd:

```
addReccasterEnvVars("BUILDING")
```

Note: There is a list of common environment variables that are sent by default. They are defined in [dbcb.c](castApp/src/dbcb.c).

### reccastTimeout

The reccastTimeout variable allows you to customize the timeout duration (in seconds) for the TCP socket connection. Once a connection is established and the TCP phase is complete, this timeout is increased by a factor of 4 for the periodic ping messages.

The default value is 20 seconds and this can be changed in your st.cmd:

```
var(reccastTimeout, 5.0)
```

### reccastMaxHoldoff

reccastMaxHoldoff sets the upper limit in seconds on how long to delay between a successful UDP search and starting the TCP phase of the reccaster protocol. There is some randomness added to the holdoff time to prevent all IOC instances from starting the TCP phase at the same time (in the event of restarting recceiver or restarting many IOCs).

The default value is 10 seconds.

```
var(reccastMaxHoldoff, 5.0)
```

#!../../bin/linux-x86_64-debug/demo

## You may have to change demo to something else
## everywhere it appears in this file

< envPaths

## Register all support components
dbLoadDatabase("../../dbd/demo.dbd",0,0)
demo_registerRecordDeviceDriver(pdbbase)

var(reccastTimeout, 5.0)
var(reccastMaxHoldoff, 5.0)

epicsEnvSet("IOCNAME", "$(IOCSH_NAME)")
epicsEnvSet("ENGINEER", "myself")
epicsEnvSet("LOCATION", "myplace")

epicsEnvSet("CONTACT", "mycontact")
epicsEnvSet("BUILDING", "mybuilding")
epicsEnvSet("SECTOR", "mysector")

addReccasterEnvVars("CONTACT", "SECTOR")
addReccasterEnvVars("BUILDING")

## Load record instances
dbLoadRecords("../../db/reccaster.db", "P=$(IOCSH_NAME):")
dbLoadRecords("../../db/somerecords.db","P=$(IOCSH_NAME):")

iocInit()

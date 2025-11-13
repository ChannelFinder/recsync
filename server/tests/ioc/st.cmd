## Register all support components
dbLoadDatabase("/recsync/dbd/demo.dbd",0,0)
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

addReccasterExcludePattern("*_", "*__")
addReccasterExcludePattern("*exclude_this")

## Load record instances
dbLoadRecords("/recsync/castApp/Db/reccaster.db", "P=$(IOCSH_NAME):")
dbLoadRecords("/recsync/demoApp/Db/somerecords.db","P=$(IOCSH_NAME):")
dbLoadRecords("/ioc/$(DB_FILE=archive.db)","P=$(IOCSH_NAME):")

iocInit()

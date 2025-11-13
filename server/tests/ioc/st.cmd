## Register all support components
dbLoadDatabase("/recsync/dbd/demo.dbd",0,0)
demo_registerRecordDeviceDriver(pdbbase)

var(reccastTimeout, 5.0)
var(reccastMaxHoldoff, 5.0)

epicsEnvSet("IOCNAME", "$(IOCSH_NAME)")

## Load record instances
dbLoadRecords("/ioc/$(DB_FILE=test.db)","P=$(IOCSH_NAME):")

iocInit()

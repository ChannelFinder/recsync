TOP=../..

include $(TOP)/configure/CONFIG
#----------------------------------------
#  ADD MACRO DEFINITIONS AFTER THIS LINE
#=============================

LIBRARY_IOC = reccaster

# Will increment with ABI changes
SHRLIB_VERSION = 0

reccaster_SRCS += sockhelpers.c
reccaster_SRCS += caster.c
reccaster_SRCS += castudp.c
reccaster_SRCS += casttcp.c
reccaster_SRCS += castinit.c
reccaster_SRCS += dbcb.c

reccaster_SYS_LIBS_WIN32 += ws2_32
reccaster_LIBS += $(EPICS_BASE_IOC_LIBS)

DBD += reccaster.dbd

PROD_LIBS += reccaster
PROD_LIBS += $(EPICS_BASE_IOC_LIBS)

TESTPROD_HOST += testsock
testsock_SRCS += testsock.c
testsock_SYS_LIBS_WIN32 = ws2_32
TESTS += testsock

TESTPROD_HOST += testudp
testudp_SRCS += testudp.c
testudp_SYS_LIBS_WIN32 = ws2_32
TESTS += testudp

TESTPROD_HOST += testtcp
testtcp_SRCS += testtcp.c
testtcp_SYS_LIBS_WIN32 = ws2_32
TESTS += testtcp

TESTPROD_HOST += testAddEnvVars
testAddEnvVars_SRCS += testAddEnvVars.c
testAddEnvVars_SYS_LIBS_WIN32 = ws2_32
TESTS += testAddEnvVars

TESTSCRIPTS_HOST += $(TESTS:%=%.t)

#===========================

include $(TOP)/configure/RULES
#----------------------------------------
#  ADD RULES AFTER THIS LINE

ifdef T_A
.PHONY: help
help:
	@echo "In $$PWD"
	@echo "TESTS: $(TESTS)"
	@echo "TESTSCRIPTS_HOST: $(TESTSCRIPTS_HOST)"
	@echo "TESTSCRIPTS: $(TESTSCRIPTS)"
endif

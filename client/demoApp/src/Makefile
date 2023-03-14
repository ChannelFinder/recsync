TOP=../..

include $(TOP)/configure/CONFIG
#----------------------------------------
#  ADD MACRO DEFINITIONS AFTER THIS LINE
#=============================

#=============================
# Build the IOC application

PROD_IOC = demo
# demo.dbd will be created and installed
DBD += demo.dbd

# demo.dbd will be made up from these files:
demo_DBD += base.dbd
demo_DBD += reccaster.dbd

# Add all the support libraries needed by this IOC
demo_LIBS += reccaster

# demo_registerRecordDeviceDriver.cpp derives from demo.dbd
demo_SRCS += demo_registerRecordDeviceDriver.cpp

# Build the main IOC entry point on workstation OSs.
demo_SRCS_DEFAULT += demoMain.cpp
demo_SRCS_vxWorks += -nil-

# Add support from base/src/vxWorks if needed
#demo_OBJS_vxWorks += $(EPICS_BASE_BIN)/vxComLibrary

# Finally link to the EPICS Base libraries
demo_LIBS += $(EPICS_BASE_IOC_LIBS)

#===========================

include $(TOP)/configure/RULES
#----------------------------------------
#  ADD RULES AFTER THIS LINE

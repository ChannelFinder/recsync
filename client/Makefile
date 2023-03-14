#Makefile at top of application tree
TOP = .
include $(TOP)/configure/CONFIG
DIRS := $(DIRS) configure
DIRS := $(DIRS) castApp
ifneq ($(SKIPDEMO),YES)
DIRS := $(DIRS) demoApp
endif
DIRS := $(DIRS) iocBoot

define DIR_template
 $(1)_DEPEND_DIRS = configure
endef
$(foreach dir, $(filter-out configure,$(DIRS)),$(eval $(call DIR_template,$(dir))))

iocBoot_DEPEND_DIRS += $(filter %App,$(DIRS))
demoApp_DEPEND_DIRS += castApp

include $(TOP)/configure/RULES_TOP

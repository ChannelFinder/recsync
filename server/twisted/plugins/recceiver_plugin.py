# -*- coding: utf-8 -*-

from recceiver import dbstore, processors
from recceiver.application import Maker
from recceiver.cf import CFProcessor

serviceMaker = Maker()

showfactory = processors.ProcessorFactory("show", processors.ShowProcessor)
dbfactory = processors.ProcessorFactory("db", dbstore.DBProcessor)
cffactory = processors.ProcessorFactory("cf", CFProcessor)

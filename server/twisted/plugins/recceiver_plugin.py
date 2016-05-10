# -*- coding: utf-8 -*-

from recceiver.application import Maker

from recceiver import processors, dbstore, cfstore

serviceMaker = Maker()

showfactory = processors.ProcessorFactory('show', processors.ShowProcessor)
cffactory = processors.ProcessorFactory('cf', cfstore.CFProcessor)
dbfactory = processors.ProcessorFactory('db',  dbstore.DBProcessor)

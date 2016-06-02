# -*- coding: utf-8 -*-

from recceiver.application import Maker

from recceiver import processors, dbstore

serviceMaker = Maker()

showfactory = processors.ProcessorFactory('show', processors.ShowProcessor)
dbfactory = processors.ProcessorFactory('db',  dbstore.DBProcessor)

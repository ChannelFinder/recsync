from recceiver import cfstore, dbstore, processors
from recceiver.application import Maker

serviceMaker = Maker()

showfactory = processors.ProcessorFactory("show", processors.ShowProcessor)
dbfactory = processors.ProcessorFactory("db", dbstore.DBProcessor)
cffactory = processors.ProcessorFactory("cf", cfstore.CFProcessor)

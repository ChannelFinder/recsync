# -*- coding: utf-8 -*-

from zope.interface import Interface, Attribute

from twisted.application import service

class ITransaction(Interface):
    
    src = Attribute('Source Address.')
    
    addrec = Attribute("""Records being added
    {recid: ('recname', 'rectype', 'recdesc', {'key':'val'})}
    """)
    
    delrec = Attribute('A set() of recids which are being removed')
    
    infos = Attribute('A dictionary of new client wide infos')
    
    recinfos = Attribute("""Additional infos being added to existing records
    recid: {'key':'val'}
    """)

class IProcessor(service.IService):

    def commit(transaction):
        """Consume and process the provided ITransaction.
        
        Returns either a Deferred or None.
        
        If a Deferred is returned the no further transactions
        will be committed until it completes.
        """

class IProcessorFactory(Interface):
    name = Attribute("A unique name identifying this factory")
    
    def build(name, opts):
        """Return a new IProcessor instance.
        
        name is the name of the instance to be created
        opts is a dictonary of configuration options
        """

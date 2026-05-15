# -*- coding: utf-8 -*-

from twisted.application import service
from zope.interface import Attribute, Interface


class ITransaction(Interface):
    source_address = Attribute("IAddress of the IOC connection (provides .host: str and .port: int)")

    records_to_add = Attribute("""Records being added
    {recid: ('recname', 'rectype')}
    """)

    records_to_delete = Attribute("A set() of recids which are being removed")

    client_infos = Attribute("A dictionary of new client wide infos")

    record_infos_to_add = Attribute("""Additional infos being added to existing records
    recid: {'key':'val'}
    """)

    aliases = Attribute("A dict mapping record id to list of alias names")

    initial = Attribute("True if this is the first transaction for this IOC connection")

    connected = Attribute("False if the IOC has disconnected")


class IProcessor(service.IService):
    def commit(self, transaction):
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

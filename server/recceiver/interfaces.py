# -*- coding: utf-8 -*-

from zope.interface import Attribute, Interface

from twisted.application import service


class ITransaction(Interface):
    source_address = Attribute("Source Address.")

    records_to_add = Attribute("""Records being added
    {recid: ('recname', 'rectype', {'key':'val'})}
    """)

    records_to_delete = Attribute("A set() of recids which are being removed")

    client_infos = Attribute("A dictionary of new client wide infos")

    record_infos_to_add = Attribute("""Additional infos being added to existing records
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

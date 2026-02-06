from dataclasses import dataclass

from twisted.application import service
from zope.interface import Attribute, Interface


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


@dataclass
class SourceAddress:
    host: str
    port: int


@dataclass
class CommitTransaction:
    source_address: SourceAddress
    srcid: int
    client_infos: dict[str, str]
    records_to_add: dict[str, tuple[str, str]]
    records_to_delete: set[str]
    record_infos_to_add: dict[str, dict[str, str]]
    aliases: dict[str, list[str]]
    initial: bool
    connected: bool


class IProcessor(service.IService):
    def commit(transaction: CommitTransaction):
        """Consume and process the provided CommitTransaction.

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

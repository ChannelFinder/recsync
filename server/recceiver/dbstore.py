from __future__ import annotations

import itertools
import logging
from typing import Any

from twisted.application import service
from twisted.enterprise import adbapi as db
from twisted.internet import defer
from zope.interface import implementer

from . import interfaces

_log = logging.getLogger(__name__)

__all__ = ["DBProcessor"]


@implementer(interfaces.IProcessor)
class DBProcessor(service.Service):
    def __init__(self, name: str, conf: Any) -> None:  # noqa: ANN401
        self.name, self.conf = name, conf
        self.Ds: set[defer.Deferred] = set()
        self.done = False
        self.tserver = self.conf.get("table.server", "server")
        self.tinfo = self.conf.get("table.info", "servinfo")
        self.trecord = self.conf.get("table.record", "record")
        self.tname = self.conf.get("table.record_name", "record_name")
        self.trecinfo = self.conf.get("table.recinfo", "recinfo")
        self.mykey = int(self.conf["idkey"])
        self.pool: db.ConnectionPool | None = None
        self.sources: dict[int, Any] = {}

    def decCount(self, _ignored: Any, D: defer.Deferred) -> None:  # noqa: ANN401
        if not self.Ds:
            msg = "Expected non-empty self.Ds"
            raise RuntimeError(msg)
        self.Ds.remove(D)
        if self.done and self.pool:
            self.pool.close()

    def waitFor(self, D: defer.Deferred) -> defer.Deferred:
        self.Ds.add(D)
        D.addBoth(self.decCount, D)
        return D

    def startService(self) -> None:
        _log.info("Start DBService")
        service.Service.startService(self)

        # map of source id# to server table id keys
        self.sources = {}

        dbargs = {}
        for arg in self.conf.get("dbargs", "").split(","):
            key_raw, _, val_raw = arg.partition("=")
            key, val = key_raw.strip(), val_raw.strip()
            if not key or not val:
                continue
            dbargs[key] = val

        if self.conf.get("dbtype") == "sqlite3" and "isolation_level" not in dbargs:
            dbargs["isolation_level"] = "IMMEDIATE"

        # workaround twisted bug #3629
        dbargs["check_same_thread"] = False

        self.pool = db.ConnectionPool(self.conf["dbtype"], self.conf["dbname"], **dbargs)

        if self.pool:
            self.waitFor(self.pool.runInteraction(self.cleanupDB))

    def stopService(self) -> defer.DeferredList:
        _log.info("Stop DBService")

        service.Service.stopService(self)

        if self.pool:
            self.waitFor(self.pool.runInteraction(self.cleanupDB))

        if not self.Ds:
            msg = "Expected non-empty self.Ds during shutdown"
            raise RuntimeError(msg)
        self.done = True
        return defer.DeferredList(list(self.Ds), consumeErrors=True)

    def cleanupDB(self, cur: Any) -> None:  # noqa: ANN401
        _log.info("Cleanup DBService")

        if self.mykey == 0:
            msg = "Expected non-zero self.mykey"
            raise RuntimeError(msg)
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute(f"DELETE FROM {self.tserver} WHERE owner=?", (self.mykey,))  # noqa: S608

    def commit(self, transaction: interfaces.CommitTransaction) -> defer.Deferred:
        if not self.pool:
            msg = "DBProcessor not started (pool is None)"
            raise RuntimeError(msg)
        return self.pool.runInteraction(self._commit, transaction)

    def _commit(self, cur: Any, transaction: interfaces.CommitTransaction) -> None:  # noqa: ANN401
        cur.execute("PRAGMA foreign_keys = ON;")

        if not transaction.initial:
            srvid = self.sources[transaction.srcid]
        else:
            cur.execute(
                f"INSERT INTO {self.tserver} (hostname,port,owner) VALUES (?,?,?)",  # noqa: S608
                (
                    transaction.source_address.host,
                    transaction.source_address.port,
                    self.mykey,
                ),
            )
            cur.execute(
                f"SELECT id FROM {self.tserver} WHERE hostname=? AND port=? AND owner=?",  # noqa: S608
                (
                    transaction.source_address.host,
                    transaction.source_address.port,
                    self.mykey,
                ),
            )
            R = cur.fetchone()
            srvid = R[0]
            self.sources[transaction.srcid] = srvid

        if not transaction.connected:
            cur.execute(
                f"DELETE FROM {self.tserver} where id=? AND owner=?",  # noqa: S608
                (srvid, self.mykey),
            )
            self.sources.pop(transaction.srcid, None)
            return

        # update client-wide client_infos
        cur.executemany(
            f"INSERT OR REPLACE INTO {self.tinfo} (host,key,value) VALUES (?,?,?)",  # noqa: S608
            [(srvid, K, V) for K, V in transaction.client_infos.items()],
        )

        # Remove all records, including those which will be re-created
        cur.executemany(
            f"DELETE FROM {self.trecord} WHERE host=? AND id=?",  # noqa: S608
            itertools.chain(
                [(srvid, recid) for recid in transaction.records_to_add],
                [(srvid, recid) for recid in transaction.records_to_delete],
            ),
        )

        # Start new records
        cur.executemany(
            f"INSERT INTO {self.trecord} (host, id, record_type) VALUES (?,?,?)",  # noqa: S608
            [(srvid, recid, record_type) for recid, (record_name, record_type) in transaction.records_to_add.items()],
        )

        # Add primary record names
        cur.executemany(
            f"""INSERT INTO {self.tname} (rec, record_name, prim) VALUES (
                         (SELECT pkey FROM {self.trecord} WHERE id=? AND host=?)
                         ,?,1)""",  # noqa: S608
            [(recid, srvid, record_name) for recid, (record_name, record_type) in transaction.records_to_add.items()],
        )

        # Add new record aliases
        cur.executemany(
            f"""INSERT INTO {self.tname} (rec, record_name, prim) VALUES (
                         (SELECT pkey FROM {self.trecord} WHERE id=? AND host=?)
                         ,?,0)""",  # noqa: S608
            [(recid, srvid, record_name) for recid, names in transaction.aliases.items() for record_name in names],
        )

        # add record client_infos
        cur.executemany(
            f"""INSERT OR REPLACE INTO {self.trecinfo} (rec,key,value) VALUES (
                         (SELECT pkey FROM {self.trecord} WHERE id=? AND host=?)
                         ,?,?)""",  # noqa: S608
            [
                (recid, srvid, K, V)
                for recid, client_infos in transaction.record_infos_to_add.items()
                for K, V in client_infos.items()
            ],
        )

# -*- coding: utf-8 -*-

import itertools

from zope.interface import implementer

from twisted.internet import defer
from twisted.application import service
from twisted.enterprise import adbapi as db

from . import interfaces

__all__  = ['DBProcessor']

@implementer(interfaces.IProcessor)
class DBProcessor(service.Service):
    def __init__(self, name, conf):
        self.name, self.conf = name, conf
        self.Ds = set()
        self.done = False
        self.tserver = self.conf.get('table.server', 'server')
        self.tinfo = self.conf.get('table.info', 'servinfo')
        self.trecord = self.conf.get('table.record', 'record')
        self.tname = self.conf.get('table.record_name', 'record_name')
        self.trecinfo = self.conf.get('table.recinfo', 'recinfo')
        self.mykey = int(self.conf['idkey'])

    def decCount(self, X, D):
        assert len(self.Ds) > 0
        self.Ds.remove(D)
        if self.done:
            self.pool.close()

    def waitFor(self, D):
        self.Ds.add(D)
        D.addBoth(self.decCount, D)
        return D

    def startService(self):
        service.Service.startService(self)

        # map of source id# to server table id keys
        self.sources = {}

        dbargs = {}
        for arg in self.conf.get('dbargs', '').split(','):
            key, _, val = arg.partition('=')
            key, val = key.strip(), val.strip()
            if not key or not val:
                continue
            dbargs[key] = val

        if self.conf['dbtype'] == 'sqlite3':
            if 'isolation_level' not in dbargs:
                dbargs['isolation_level'] = 'IMMEDIATE'

        # workaround twisted bug #3629
        dbargs['check_same_thread'] = False

        self.pool = db.ConnectionPool(self.conf['dbtype'],
                                      self.conf['dbname'],
                                      **dbargs)

        self.waitFor(self.pool.runInteraction(self.cleanupDB))

    def stopService(self):
        service.Service.stopService(self)

        self.waitFor(self.pool.runInteraction(self.cleanupDB))

        assert len(self.Ds) > 0
        self.done = True
        return defer.DeferredList(list(self.Ds), consumeErrors=True)

    def cleanupDB(self, cur):
        assert self.mykey != 0
        cur.execute('PRAGMA foreign_keys = ON;')
        cur.execute('DELETE FROM %s WHERE owner=?' % self.tserver,
                    self.mykey)

    def commit(self, TR):
        return self.pool.runInteraction(self._commit, TR)

    def _commit(self, cur, TR):
        cur.execute('PRAGMA foreign_keys = ON;')

        if not TR.initial:
            srvid = self.sources[TR.srcid]
        else:
            cur.execute('INSERT INTO %s (hostname,port,owner) VALUES (?,?,?)' % self.tserver,
                        (TR.src.host, TR.src.port, self.mykey))
            cur.execute('SELECT id FROM %s WHERE hostname=? AND port=? AND owner=?' % self.tserver,
                        (TR.src.host, TR.src.port, self.mykey))
            R = cur.fetchone()
            srvid = R[0]
            self.sources[TR.srcid] = srvid

        if not TR.connected:
            cur.execute('DELETE FROM %s where id=? AND owner=?' % self.tserver,
                        (srvid, self.mykey))
            del self.sources[TR.srcid]
            return

        # update client-wide infos
        cur.executemany('INSERT OR REPLACE INTO %s (host,key,value) VALUES (?,?,?)' % self.tinfo,
                        [(srvid, K, V) for K, V in TR.infos.items()])

        # Remove all records, including those which will be re-created
        cur.executemany('DELETE FROM %s WHERE host=? AND id=?' % self.trecord,
                        itertools.chain(
                            [(srvid, recid) for recid in TR.addrec],
                            [(srvid, recid) for recid in TR.delrec]
                        ))

        # Start new records
        cur.executemany('INSERT INTO %s (host, id, rtype, rdesc) VALUES (?,?,?,?)' % self.trecord,
                        [(srvid, recid, rtype, rdesc) for recid, (rname, rtype, rdesc) in TR.addrec.items()])

        # Add primary record names
        cur.executemany("""INSERT INTO %s (rec, rname, prim) VALUES (
                         (SELECT pkey FROM %s WHERE id=? AND host=?)
                         ,?,1)""" % (self.tname, self.trecord),
                        [(recid, srvid, rname) for recid, (rname, rtype, rdesc) in TR.addrec.items()])

        # Add new record aliases
        cur.executemany("""INSERT INTO %(name)s (rec, rname, prim) VALUES (
                         (SELECT pkey FROM %(rec)s WHERE id=? AND host=?)
                         ,?,0)""" % {'name': self.tname, 'rec': self.trecord},
                        [(recid, srvid, rname)
                         for recid, names in TR.aliases.items()
                         for rname, rdesc in names
                         ])

        # add record infos
        cur.executemany("""INSERT OR REPLACE INTO %s (rec,key,value) VALUES (
                         (SELECT pkey FROM %s WHERE id=? AND host=?)
                         ,?,?)""" % (self.trecinfo, self.trecord),
                        [(recid, srvid, K, V)
                         for recid, infos in TR.recinfos.items()
                         for K, V in infos.items()
                         ])


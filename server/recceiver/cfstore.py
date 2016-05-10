# -*- coding: utf-8 -*-

import itertools

from zope.interface import implements
from twisted.internet import defer
from twisted.application import service
from twisted.enterprise import adbapi as db
from channelfinder import ChannelFinderClient
import interfaces
import datetime

# ITRANSACTION FORMAT:
#
# src = source address
# addrec = records ein added ( recname, rectype, {key:val})
# delrec = a set() of records which are being removed
# infos = dictionary of client infos
# recinfos = additional infos being added to existing records 
# "recid: {key:vale}"
#


__all__  = ['CFProcessor']

class CFProcessor(service.Service):
    implements(interfaces.IProcessor)

    def __init__(self, name, conf):
        print "CF_INIT"
        self.name,self.conf = name,conf
        print "CONF"+str(conf)
        
        
    def startService(self):
        service.Service.startService(self)
        self.running = 1
        print "CF_START"
        try:
            '''
            Using the default python cf-client.
            The usr, username, and password are provided by the channelfinder._conf module.
            '''
            self.client = ChannelFinderClient()
        except:
            print 'Failed to create cf client'
        
    def stopService(self):
        service.Service.stopService(self)
        #Set channels to inactive and close connection to client
        self.running = 0
        print "CF_STOP"
        
    def execute(self, cmd):
        pass

    def add(self, rec):
        pass
    
    def delete(self, rec):
        pass
    
    def commit(self, TR):
        print "CF_COMMIT"
#    print TR.src
#    print TR.src.host, TR.src.port
        print [(K,V) for K,V in TR.infos.iteritems()]
        pvNames = [unicode(rname, "utf-8") for rid, (rname, rtype) in TR.addrec.iteritems()]
        iocName=TR.src.port
        hostName=TR.src.host
        owner='cfstore'
    
        '''
        Currently using the hostIP and the iocPort
        if 'IOCNAME' in TR.infos:
                iocName = TR.infos['IOCNAME']
        if 'HOSTNAME' in TR.infos:
                hostName = TR.infos['HOSTNAME']
        '''
        if 'ENGINEER' in TR.infos:
            owner = TR.infos['ENGINEER']
        time = str(datetime.datetime.now())
            
        if iocName and hostName and owner:
                updateChannelFinder(self.client, pvNames, hostName, iocName, time, owner)
        else:
            print 'failed to initialize one or more of the following properties \
                hostname:',hostName,', iocname:',iocName,', owner:',owner 
            
def updateChannelFinder(client, pvNames, hostName, iocName, time, owner):
    '''
    pvNames = list of pvNames 
    ([] permitted will effectively remove the hostname, iocname from all channels)
    hostName = pv hostName (None not permitted)
    iocName = pv iocName (None not permitted)
    owner = the owner of the channels and properties being added, this can be different from the user
    e.g. user = abc might create a channel with owner = group-abc
    time = the time at which these channels are being created/modified
    '''
    if hostName == None or iocName == None:
        raise Exception, 'missing hostName or iocName'
    channels = []
    checkPropertiesExist(client, owner)
    previousChannelsList = client.findByArgs([('hostName', hostName), ('iocName', iocName)])
    if previousChannelsList != None:
        for ch in previousChannelsList:
#        print 'found channel:', ch[u'name'] in pvNames, ch[u'name']
            if pvNames != None and ch[u'name'] in pvNames:
                ''''''
                channels.append(updateChannel(ch,\
                                              owner=owner, \
                                              hostName=hostName, \
                                              iocName=iocName, \
                                              pvStatus='Active', \
                                              time=time))
                pvNames.remove(ch[u'name'])
            elif pvNames == None or ch[u'name'] not in pvNames:
                '''Orphan the channel : mark as inactive, keep the old hostName and iocName'''
                oldHostName = hostName
                oldIocName = iocName
                oldTime = time
                for prop in ch[u'properties']:
                    if prop[u'name'] == u'hostName':
                        oldHostName = prop[u'value']
                    if prop[u'name'] == u'iocName':
                        oldIocName = prop[u'value']
                    if prop[u'name'] == u'time':
                        oldTime = prop[u'value']
#        print oldHostName, oldIocName, oldTime
                channels.append(updateChannel(ch, \
                                owner=owner, \
                                hostName=oldHostName, \
                                iocName=oldIocName, \
                                pvStatus='InActive', \
                                time=oldTime))

    # now pvNames contains a list of pv's new on this host/ioc
    for pv in pvNames:
        ch = client.findByArgs([('~name',pv)])
        if not ch:
            '''New channel'''
            channels.append(createChannel(pv, \
                                          chOwner=owner, \
                                          hostName=hostName, \
                                          iocName=iocName, \
                                          pvStatus='Active', \
                                          time=time))
        elif len(ch) == 1:
            '''update existing channel: exists but with a different hostName and/or iocName'''
            channels.append(updateChannel(ch[0], \
                                          owner=owner, \
                                          hostName=hostName, \
                                          iocName=iocName, \
                                          pvStatus='Active', \
                                          time=time))
    client.set(channels=channels)

def updateChannel(channel, owner, hostName=None, iocName=None, pvStatus='InActive', time=None):
    '''
    Helper to update a channel object so as to not affect the existing properties
    '''
    # properties list devoid of hostName and iocName properties
    if channel[u'properties']:
        channel[u'properties'] = [property for property in channel[u'properties'] \
                         if property[u'name'] != 'hostName' \
                         and property[u'name'] != 'iocName'\
                         and property[u'name'] != 'pvStatus']
    else:
       channel[u'properties'] = []
    if hostName != None:
        channel[u'properties'].append({u'name' : 'hostName', u'owner' : owner, u'value': hostName})
    if iocName != None:
        channel[u'properties'].append({u'name' : 'iocName', u'owner' : owner, u'value': iocName})
    if pvStatus:
        channel[u'properties'].append({u'name' : 'pvStatus', u'owner' : owner, u'value': pvStatus})
    if time:
        channel[u'properties'].append({u'name' : 'time', u'owner' : owner, u'value': time})
    return channel

def createChannel(chName, chOwner, hostName=None, iocName=None, pvStatus='InActive', time=None):
    '''
    Helper to create a channel object with the required properties
    '''
    ch = {u'name':chName,u'owner':chOwner,u'properties':[]}
    if hostName != None:
        ch[u'properties'].append({u'name' : 'hostName', u'owner' : chOwner, u'value': hostName})
    if iocName != None:
        ch[u'properties'].append({u'name' : 'iocName', u'owner' : chOwner, u'value': iocName})
    if pvStatus:
        ch[u'properties'].append({u'name' : 'pvStatus', u'owner' : chOwner, u'value': pvStatus})
    if time:
        ch[u'properties'].append({u'name' : 'time', u'owner' : chOwner, u'value': time})
    return ch

def checkPropertiesExist(client, propOwner):
    '''
    Checks if the properties used by dbUpdate are present if not it creates them
    '''
    requiredProperties = ['hostName', 'iocName', 'pvStatus', 'time']
    for propName in requiredProperties:
        if client.findProperty(propName) == None:
            try:
                client.set(property={u'name': propName, u'owner':propOwner})
            except Exception as e:
                print 'Failed to create the property',propName
                print 'CAUSE:',e.message


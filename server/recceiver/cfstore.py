# -*- coding: utf-8 -*-

import itertools

from zope.interface import implements
from twisted.internet import defer
from twisted.application import service
from twisted.enterprise import adbapi as db
from channelfinder import ChannelFinderClient
from channelfinder import _conf
#from _conf import _conf
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
#


__all__  = ['CFProcessor']

class CFProcessor(service.Service):
    implements(interfaces.IProcessor)

    def __init__(self, name, conf):
        print "CF_INIT"
        self.name,self.conf = name,conf
        print "CONF"+str(conf)
        
        
    def startService(self): #event trigger on service start
        service.Service.startService(self)
        self.running = 1
        print "CF_START"
        client = 0 #CHANNELFINDERCLIENTINIT
        #__baseURL = _conf.__getDefaultConfig('BaseURL',BaseURL)
        #__userName = _conf.__getDefaultConfig('username',username)
        #__password = _conf.__getDefaultConfig('password',password)
        #except:
        #    raise Exception, 'Failed to create client to '+__baseURL
        #print str(__baseURL) + str(__userName) + str(__password)
        
        #TODO: initialize the pycfclient here

    def stopService(self):
        service.Service.stopService(self)
        #Set channels to inactive and close connection to client
        self.running = 0
        print "CF_STOP"
        
    def execute(self, cmd):
        pass #execute command on softioc

    def add(self, rec):
        pass #TODO: this must be replaced with
    #the varius cfupdate methods which are related to
    # channelfinderclient from pycfc
    
    def delete(self, rec):
        pass
    
    def commit(self, TR):
        print "CF_COMMIT"
        
        #print TR.addrec #{327680: ('test:li', 'longin'), 393216: ('test:lo', 'longout'), 458752: ('test:State-Sts', 'mbbi'), 524288: ('test:Msg-I', 'stringin')}
        #print "del: " + str(TR.delrec) #set([])
        #print "infos: " + str(TR.infos[0]) #{'LOCATION': 'myplace', 'PWD': '/home/mike/git/recsync/client/iocBoot/iocdemo', 'IOCNAME': 'myioc', 'EPICS_VERSION': 'EPICS 3.14.12.3-8', 'ENGINEER': 'myself'}
        #print "recinfos:" + str(TR.recinfos[0]) #{1441792: {'test': 'testing', 'hello': 'world'}}
        pvNames = []
        for key in TR.addrec:
            pvNames.append(TR.addrec[key][0])
        print "PV NAMES" + str(pvNames)
        #print 
        
        iocName = TR.infos['IOCNAME']
        hostName = TR.infos['LOCATION']
        time = datetime.datetime.now().time()
        owner = "fix me" #username from configs
        service = "https://192.168.56.1:8181/ChannelFinder" #change IP 
        username = "cf-update"
        password = "1234"
        
        updateChannelFinder(pvNames, hostName, iocName, time, owner, service, username, password)
        
        #for rec in TR.delrec:
        #    delete(rec)
        #infos, recinfos
        
def updateChannelFinder(pvNames, hostName, iocName, time, owner, \
                        service=None, username=None, password=None):
    '''
    pvNames = list of pvNames 
    ([] permitted will effectively remove the hostname, iocname from all channels)
    hostName = pv hostName (None not permitted)
    iocName = pv iocName (None not permitted)
    owner = the owner of the channels and properties being added, this can be different from the user
    e.g. user = abc might create a channel with owner = group-abc
    time = the time at which these channels are being created/modified
    [optional] if not specified the default values are used by the 
    channelfinderapi lib
    service = channelfinder service URL
    username = channelfinder username
    password = channelfinder password
    '''
    if hostName == None or iocName == None:
        raise Exception, 'missing hostName or iocName'
    channels = []
    try:
        client = ChannelFinderClient(BaseURL=service, username=username, password=password)
    except:
        raise Exception, 'Unable to create a valid webResourceClient'
    checkPropertiesExist(client, owner)
    previousChannelsList = client.findByArgs([('hostName', hostName), ('iocName', iocName)])
    if previousChannelsList != None:
        for ch in previousChannelsList:
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
                channels.append(updateChannel(ch, \
                                              owner=owner, \
                                              hostName=ch.getProperties()['hostName'], \
                                              iocName=ch.getProperties()['iocName'], \
                                              pvStatus='InActive', \
                                              time=ch.getProperties()['time']))
    # now pvNames contains a list of pv's new on this host/ioc
    for pv in pvNames:
        ch = client.findByArgs([('~name',pv)])
        if ch == None:
            '''New channel'''
            channels.append(createChannel(pv, \
                                          chOwner=owner, \
                                          hostName=hostName, \
                                          iocName=iocName, \
                                          pvStatus='Active', \
                                          time=time))
        elif ch[0] != None:
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
    if channel['props']:
        channel['props'] = [property for property in channel['props'] \
                         if 'hostName' in channel['props'] and 'iocName' in channel['props'] and 'pvStatus' in channel['props']]
    else:
       ch['props'] = {}
    if hostName != None:
        ch['props']['hostname'] = hostName
    if iocName != None:
        ch['props']['iocName'] = iocName
    if pvStatus:
        ch['props']['pvStatus'] = pvStatus
    if time:
        ch['props']['time'] = time
    return channel

def createChannel(chName, chOwner, hostName=None, iocName=None, pvStatus='InActive', time=None):
    '''
    Helper to create a channel object with the required properties
    '''
    
    ch = {"name":chName,"owner":chOwner}
    ch['props'] = {}
    if hostName != None:
        ch['props']['hostname'] = hostName
    if iocName != None:
        ch['props']['iocName'] = iocName
    if pvStatus:
        ch['props']['pvStatus'] = pvStatus
    if time:
        ch['props']['time'] = time
    return ch

def checkPropertiesExist(client, propOwner):
    '''
    Checks if the properties used by dbUpdate are present if not it creates them
    '''
    requiredProperties = ['hostName', 'iocName', 'pvStatus', 'time']
    for propName in requiredProperties:
        if client.findProperty(propName) == None:
            try:
                client.set(property={"name": propName, "owner":propOwner})
            except Exception as e:
                print 'Failed to create the property',propName
                print 'CAUSE:',e.message

        
        


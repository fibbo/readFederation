import sys
import os
import string
import xml.etree.ElementTree as ET
import gfal2
import errno
import time
import cPickle as pickle
from timer import Timer

##### remove this part once it's integrated as agent #
from DIRAC.Core.Base.Script import parseCommandLine
from symbol import parameters
parseCommandLine()
######################################################


from DIRAC import gLogger, S_ERROR, S_OK

import pdb

from DIRAC                                          import gConfig
from DIRAC.Resources.Catalog.FileCatalog            import FileCatalog
from DIRAC.Resources.Storage.StorageElement         import StorageElement
from DIRAC.Core.Base.AgentModule                    import AgentModule
from DIRAC.Core.Utilities.Pfn                       import pfnparse, pfnunparse
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

from stat import S_ISREG, S_ISDIR, S_IXUSR, S_IRUSR, S_IWUSR, \
  S_IRWXG, S_IRWXU, S_IRWXO


class catalogAgent( object ):

  def initialize( self ):
    """
    Setting up the crawler with a gfal2 context, a file catalog handle and an empty file dict
    :param self: self reference
    """

    self.log = gLogger.getSubLogger( "readFederation", True )
    self.gfal2  = gfal2.creat_context()
    self.rootURL = 'http://federation.desy.de/fed/lhcb/LHCb/Collision12_25/BHADRON.MDST/00022722/0000/'
    self.dedicatedSE = None #['CNAF_M-DST','IN2P3_M-DST']
    # self.rootURL = 'http://federation.desy.de/fed/lhcb/LHCb/Collision10/BHADRON.DST/00010938/0000'
    self.fileList = []
    self.history = []
    self.SEDict = self.__instantiateSEs()
    self.successfulFiles = {}
    self.failedFiles = {}
    self.failedSE = {}
    self.failedDirectories = []
    self.scannedFiles = 0
    self.scannedDirectories = 0
    

    #if a gfal2 operation fails for other reasons than NOEXIST we try again in 4 seconds
    self.sleepTime = 0
    #maximum number of tries that gfal2 takes to get information form a server
    self.max_tries = 10

    self.recursionLevel = 0


    # check if there is a checkpoint file, if yes read it as last history
    self.log.debug("readFederation.initialize: Loading checkpoint if available.")
    if os.path.isfile('checkpoint.txt'):
      self.log.debug("readFederation.initialize: Loading checkpoint.")
      with open('checkpoint.txt') as f:
        self.history = f.read().splitlines()

  def __instantiateSEs(self):
    """ Get storage config for all the SE in the dirac config. """

    SEDict = {}
    configPath = 'Resources/StorageElements'
    res = gConfig.getSections(configPath)
    if not res['OK']:
      return S_ERROR('Not able to get StorageConfig: %s') % res['Message']
    SEList = res['Value']
    for SE in SEList:
      SEDict[SE] = StorageElement( SE, protocols='GFAL2_HTTP')

    return SEDict



  def execute( self ):
    """
    Run the crawler
    :param self: self reference
    """
    res = self.__crawl( self.rootURL )
    return res


  def __crawl( self, basepath ):
    """ Crawler, starts with the first call from the rootURL and goes on from there. 

    * List all the content of the current directory and stat-call each entry and put files in the files list and directories 
      in the directories list. 
    * For all files retrieve the XML data and extract the PFNs and add them to the fileList.
    * Once all file PFNs have been extracted and the fileList is still small enough, go a directory deeper
    * If the fileList is big enough, compare each file with the file catalog
    * Possible: Once we leave a directory, add the path to a file so in case the crawler crashes, we won't check
      this directory again

    :param self: self reference
    :param str basepath: path that we want to the the information from
    """

    if self.recursionLevel == len(self.history):
      self.history.append( os.path.basename( basepath ) )

      # write the last path we visited into 'checkpoint.txt'
      self.__writeCheckPoint()
    self.recursionLevel += 1

    # if two folders with the same name are on the same level but different branches this will not be a good enough condition
    # needs improvement
    caught_up = (self.recursionLevel == len(self.history)) # and (basepath == self.history[-1])

    directories = []
    res = self.__listDirectory( basepath )
    if res['OK']:
      entries = res['Value']
    else:
      entries = []
    self.log.notice("readFederation.__crawl: stating entries.")
    
    for entry in entries:
      path = os.path.join( basepath, entry )
      dav_path = "dav" + path[4:]
      res = self.__isFile( dav_path )
      if not res['OK']:
        self.failedFiles[path] = "read_Federation.__crawl: %s" % res['Message']
        continue
  
        
      # if res['Value'] is true then it's a file  
      if res['Value'] and caught_up:
        res = self.__readFile( path )
        if not res['OK']:
          self.failedFiles[path] = "read_Federation.__readFiles: %s" % res['Message']
        if 'Value' in res.keys():
          xml_string = res['Value']
          PFNs = self.__extractPFNs( xml_string )
          self.fileList.append(PFNs)
          self.scannedFiles += 1

        #only for debugging the compareDictWithCatalog method, remove following line
        # when done
        #self.__compareFileListWithCatalog()

      elif not res['Value']:
        directories.append( entry )

    #sorting the directories so with the checkpoint we know which one have already been checked.
    directories.sort(key=lambda x: x.lower())

    if len(self.fileList) > 40:
      if not self.dedicatedSE:
        res = self.__compareFileListWithCatalog()
      else:
        res = self.__compareForDedicatedSE()
      if res['OK']:
        self.__mergeDictionaries(res['Value'])


    for directory in directories:
      if self.recursionLevel < len(self.history):
        # we are still catching up, only the last directory before the crash
        # and the later directories will be scanned.
        if directory >= self.history[self.recursionLevel]:
          self.__crawl( os.path.join( basepath, directory ) )
      else:
        self.__crawl( os.path.join(basepath, directory ) )

    # getting out from the recursion here, pop the last entry from
    # the list and reduce recursionLevel
    if len(self.history):
      self.history.pop()
    self.scannedDirectories += 1

    self.recursionLevel -= 1
    self.log.notice( "readFederation.__crawl: Current recursion level: %s" % self.recursionLevel )
    if self.recursionLevel == 0:
      if not self.dedicatedSE:
        res = self.__compareFileListWithCatalog()
      else:
        res = self.__compareForDedicatedSE()
      if res['OK']:
        self.__mergeDictionaries(res['Value'])
      try:
        os.remove('checkpoint.txt')
      except Exception, e:
        self.log.error("readFederation.__crawl: Failed to remove checkpoint")
      return S_OK( {'Failed' : self.failedFiles, 'Successful' : self.successfulFiles, 'Failed SE' : self.failedSE } )

  def __mergeDictionaries(self, res):
    self.successfulFiles.update(res['Successful'])
    self.failedFiles.update(res['Failed'])
    self.failedSE.update(res['Failed SE'])

  def __listDirectory(self, path ):
    """ Listing the directory.

    param self: self reference
    param str path: path to be listed.
    returns S_ERROR: if the path doesn't exist
            S_OK( entries ) entries are the contents of the directory

    """

    self.log.notice("readFederation.__listDirectory: Listing the current directory: %s" % path)
    entries = []
    tries = 0
    while tries < self.max_tries:
      try:
        entries = self.gfal2.listdir( path )
        return S_OK( entries )
      except gfal2.GError, e:
        if e.code == errno.ENOENT:
          return S_ERROR( 'readFederation.__listDirectory: Path %s doesnt exist' % path )
        else:
          self.log.debug('readFederation.__listDirectory: Failed to list directory [%d]: %s. Waiting %s seconds' % (e.code, e.message, self.sleepTime))
          tries += 1
          time.sleep(self.sleepTime)
    return S_ERROR( 'readFederation.__listDirectory: Failed to list directory [%d]: %s.' % (e.code, e.message) )

    

  def __writeCheckPoint( self ):
    """ Write down the folders we are visting to the checkpoint.txt file. So if we are in /A/B/C the checkpoint looks like this:
    A
    B
    C

    :param self: self reference
    :returns nothing

    """
    self.log.debug("readFederation.__writeCheckPoint: Updating checkpoint")
    f = open('checkpoint.txt', 'w')
    for entry in self.history:
      try:
        f.write(entry+'\n')
      except Exception, e:
        self.log.debug("readFederation.__writeCheckPoint: Something went wrong while writing to the checkpoint file: [%d]: %s" % \
                                                                                                              (e.code, e.message))
    f.close()

  def __isFile( self, path ):
    """ stat the file and check if the path is a file or not

    :param self: self reference
    :param str path: path to be checked
    :returns S_OK( bool ) whether the path is a file or not
             S_ERROR( errMsg ) if either the file doesn't exist or for another reason stating fails


    """
    # self.log.debug("readFederation: Checking if %s is a file or not" % path)
    tries = 0
    while tries < self.max_tries:
      try:
        statInfo = self.gfal2.stat( path )
        self.log.debug("readFederation.__isFile: stating worked")
        return S_OK( S_ISREG( statInfo.st_mode ) )
      except gfal2.GError, e:
        if e.compareDictWithCataloge == errno.ENOENT:
          errMsg = "readFederation.__isFile: File does not exist."
          self.log.debug("readFederation.__isFile: File %s does not exist" % path)
          return S_ERROR( errMsg )
        elif e.code == errno.EHOSTDOWN:
          errMsg = "readFederation.__isFile: Host unreachable: %s (%s)" % (e.message, path)
          self.log.debug(errMsg)
          return S_ERROR( errMsg )
        else:
          tries += 1
          self.log.debug("readFederation: Failed to check file: (%s,%s), trying again." % (e.code, e.message))
          time.sleep(self.sleepTime)
      

    return S_ERROR( "Couldn't check path, stopped trying after %s tries" % self.max_tries )

  def __compareFileListWithCatalog( self ):
    """ At this point we need to check for all the entries in the self.fileList if they are also in the catalog or not. The self.fileList
    represents the state on the storages and the catalog needs to match that.
    Depending on the number of replicas each entry in the fileList has one or more entries. For each sublist in fileList we generate the LFN.
    With that LFN we poll the catalog to find out what the catalog knows about this file. From that information we extract the SE where the
    catalog thinks the files are stored on.
    For each SE we intialize a StorageElement object which we save in a dict so if we need to use that particular SE more than once we don't
    initialize over and over again.
    Then for each SE we get the transport URL and check if one of the entries from urlList (which itself is an entry from self.fileList) matches
    with the transport URL from the SE then the catalog knows about that file which is good. In any case the entry is removed from the urlList
    and either put to the successful or failed dict.
    If we fail to instantiate a SE all the transportURL queries will fail so we put those LFNs that poll that SE also in the failed with that
    message.

    :param self: self reference
    :returns nothing
    """
  
    self.log.notice("readFederation.__compareFileListWithCatalog: Compare catalog with federation entries")
    failed = {}
    failedSE = {}
    successful = {}
    dmScript = DMScript()
    for urlList in self.fileList:
      self.log.debug("readFederation: Retrieving LFN for %s" % urlList)
      lfn = dmScript.getLFNsFromList( urlList )
      if not len(lfn):
        self.log.error("readFederation.__compareFileListWithCatalog: can't get LFN from HTTP URL %s" % urlList)
        continue
      # all the urls in urlList have the same LFN, it's the same file.
      lfn = lfn[0]
      # We need to put the lfn in a dict otherwise getReplicas doesn't work.
      lfnDict = {lfn : True}

      res = self.__getSEListFromReplicas(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        SEList = res['Value']
         
      # url holds all the urls that we need to check if they are also in the catalog so we compare if 
      # any of the url from url is the same
      tURLList = []
      for SE in SEList:
        se = self.SEDict.get(SE,None)
        if se:
          res = se.getURL(lfn, protocol='http')
          if not res['OK']:
            # couldn't get transport URL (for example if the SE wasn't properly instantiated)
            if not SE in failedSE:
              failedSE[SE] = "Error while getting TURL: %s" % res['Message']
          else:
            if 'Successful' in res['Value']:
              tURLList.append(res['Value']['Successful'].values()[0])
      while len(urlList):
        url = urlList.pop()
        if tURLList:
          if any(self.__compareURLS(tURL, url) for tURL in tURLList):
            # http entry found in the catalog
            continue
          else:
            if lfn in failed:
              failed[lfn].append({url : 'Failed to find match in catalog'})
            else:
              failed[lfn] = [{url : 'Failed to find match in catalog'}]
        else:
          failed[lfn] = [{url : "Unable to retrieve any valid TURL. Probably because I wasn't able to instantiate any SE for this LFN"}]

    self.fileList = []
    return S_OK( { 'Successful' : successful, 'Failed' : failed, 'Failed SE' : failedSE } )

  def __getSEListFromReplicas(self, lfn):
    fc = FileCatalog()
    lfnDict = {lfn : True}
    res = fc.getReplicas(lfnDict)
    if not res['OK']:
      self.log.debug("readFederation.__compareFileListWithCatalog: Completely failed to get Replicas")
      return S_ERROR("getReplicas: %s" % res['Message'])
    
    res = res['Value']
    if not lfn in res['Successful']:
      self.log.debug("readFederation.__compareFileListWithCatalog: Failed to get Replicas")
      return S_ERROR("getReplicas: %s" % res['Failed'][lfn])
    
    # we have a list of replicas for a given LFN. SEList contains all the SE
    # that store that file according to the catalog
    return S_OK(res['Successful'][lfn].keys())

  def __compareForDedicatedSE(self):
    self.log.notice("readFederation.__compareForDedicatedSE: Compare specific SE with federation entries")
    failed = {}
    failedSE = {}
    successful = {}
    dmScript = DMScript()
    for urlList in self.fileList:
      self.log.debug("readFederation: Retrieving LFN for %s" % urlList)
      lfn = dmScript.getLFNsFromList( urlList )
      if not len(lfn):
        self.log.error("readFederation.__compareFileListWithCatalog: can't get LFN from HTTP URL %s" % urlList)
        continue
      # all the urls in urlList have the same LFN, it's the same file.
      lfn = lfn[0]

      tURLsFromDedicatedSE = []  
      for SE in self.dedicatedSE:
        se = self.SEDict[SE]
        res = se.getURL(lfn, protocol='http')
        if not res['OK']:
          # couldn't get transport URL (for example if the SE wasn't properly instantiated)
          if not SE in failedSE:
            failedSE[SE] = "Error while getting TURL: %s" % res['Message']
        else:
          if 'Successful' in res['Value']:
            tURLsFromDedicatedSE.append( (res['Value']['Successful'].values()[0], SE) )
      
      confirmedSE = []
      for url in urlList:
        for tURL, SE in tURLsFromDedicatedSE:
          if self.__compareURLS(url, tURL):
            confirmedSE.append(SE)

      res = self.__getSEListFromReplicas(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        SEList = res['Value']
        for SE in confirmedSE:
          if SE not in SEList:
            if lfn in failed:
              failed[lfn].append({'Failed to find match in catalog for %s' % SE})
            else:
              failed[lfn] = [{'Failed to find match in catalog for %s' % SE}]
          else:
            successful[lfn] = True
    self.fileList = []
    return S_OK( { 'Successful' : successful, 'Failed' : failed, 'Failed SE' : failedSE } )


  def __compareURLS( self, fc_url, fed_url ):
    """ This method compares URLs, but should also consider, that maybe one URL doesn't have a port specified while the other has
    It is assumed that both URLs at least have protocol, host, path and filename defined.

    :param self: self reference
    :param list fc_url: 
    """
    self.log.debug("readFederation.__compareURLS: comparing TURL from SE with TURL from federation")
    fc_res = pfnparse(fc_url)['Value']
    fed_res = pfnparse(fed_url)['Value']
    key_list = ['Path', 'Filename', 'Port', 'Protocol', 'Host', 'WSUrl']
    isAMatch = True
    for key in key_list:
      fc_value = fc_res.get(key)
      fed_value = fed_res.get(key)
        # both keys have to exist and if their values are not the same then the url is not the same either
        # it is enough if one key is part of the other because sometimes configuration of a SE is
        # different to the convention
      if fc_value and fed_value:
        if not ((fc_value in fed_value) or (fed_value in fc_value)):
          isAMatch = False
          break

    return isAMatch



  def __readFile( self, afile ):
    """ Read the xml data from the file. Using gfal2.open to open file and read
        the content and write it into a string.

        :param self: self reference
        :param str filename: name of the metalink file to read
        :return S_OK( xml_string ): a string containing the xml information of file
                S_ERROR( errMsg ): if the file doesn't exist or it failed to read it.
    """
    # open the file
    afile = afile+'?metalink'
    tries = 0
    successful = False
    while tries < self.max_tries:
      try:
        f = self.gfal2.open(afile, 'r')
        successful = True
        break
      except gfal2.GError, e:
        if e.code == errno.ENOENT:
          return S_ERROR( 'File does not exist' )
        else:
          tries += 1
          self.log.debug("readFederation: Failed to read file: (%s,%s), trying again." % (e.code, e.message))
          time.sleep(self.sleepTime)
    if not successful:
      return S_ERROR("readFederation: Failed to read file (%s,%s)" % (e.code, e.message))
    try:
      xml_string = f.read(10000)
    except Exception, e:
      self.log.debug("readFederation.__readFile: Failed to read file.")
      return S_ERROR("Wasn't able to read file: [%d]: %s" % (e.code, e.message))
    return S_OK( xml_string )


  def __extractPFNs( self, xml_string ):
    """ Extract the url elements of the xml string

    :param self: self reference
    :param str xml_string: string containing the xml information of the file
    :return list PFNs: list of the pfns for each url element.

    """
    PFNs = []
    root = ET.fromstring( xml_string )
    urls = root.findall('.//{http://www.metalinker.org/}url')
    for url in urls:
      if url.text is not None:
        PFNs.append( url.text )

    return PFNs



if __name__ == '__main__':  
  CA = catalogAgent()
  gLogger.setLevel("NOTICE")
  CA.initialize()
  res = CA.execute()
  pickle.dump(res['Value'], open('crawlResults.pkl', 'wb'))
  print "Crawl finished."
  #print CA._catalogAgent__readFile( 'http://federation.desy.de/fed/lhcb/data/2009/RAW/FULL/LHCb/BEAM1/62426/062426_0000000001.raw' )
  #print CA._catalogAgent__isFile( 'http://federation.desy.de/fed/lhcb/data/2009/RAW/FULL/LHCb/BEAM1/62426/' )


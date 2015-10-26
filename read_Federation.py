import sys
import os
import string
import xml.etree.ElementTree as ET
import gfal2
import errno
import time

##### remove this part once it's integrated as agent #
from DIRAC.Core.Base.Script import parseCommandLine
from symbol import parameters
parseCommandLine()
######################################################


from DIRAC import gLogger, S_ERROR, S_OK

import pdb

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
    self.rootURL = 'http://federation.desy.de/fed/lhcb/LHCb/Collision10/BHADRON.DST/00010920/0000/'
    self.fileDict = {}
    self.history = []

    self.failedFiles = []
    self.failedDirectories = []
    self.failedEntries = []
    self.sleepTime = 4
    #self.checkPoint = checkPoint

    self.recursionLevel = 0
    self.max_tries = 10

    # check if there is a checkpoint file, if yes read it as last history
    if os.path.isfile('checkpoint.txt'):
      with open('checkpoint.txt') as f:
        self.history = f.read().splitlines()

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
    * For all files retrieve the XML data and extract the PFNs and add them to the fileDict.
    * Once all file PFNs have been extracted and the fileDict is still small enough, go a directory deeper
    * If the fileDict is big enough, compare each file with the file catalog
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

    caught_up = self.recursionLevel == len(self.history)

    directories = []
    entries = []
    tries = 0
    while True and tries < self.max_tries:
      try:
        entries = self.gfal2.listdir( basepath )
        break
      except gfal2.GError, e:
        if e.code == errno.ENOENT:
          break
        else:
          tries += 1
          time.sleep(self.sleepTime)

    for entry in entries:
      path = os.path.join( basepath, entry )
      res = self.__isFile( path )
      if not res['OK']:
        self.failedFiles.append( {res['Message'][0] : res['Message'][1]} )
        continue
      
      # if res['Value'] is true then it's a file  
      if res['Value']:
        if caught_up:
          res = self.__readFile( path )
          if not res['OK']:
            self.failedFiles[ {path : 'Failed to read xml data.'}]
          xml_string = res['Value']
          PFNs = self.__extractPFNs( xml_string )
          self.fileDict[path] = PFNs
          #only for debugging the compareDictWithCatalog method, remove following line
          # when done
          self.__compareDictWithCatalog()

      else:
        directories.append( entry )

    #sorting the directories so with the checkpoint we know which one have already been checked.
    directories.sort(key=lambda x: x.lower())

    if len(self.fileDict) > 40:
      res = self.__compareDictWithCatalog()
      if res['OK']:
        res = res['Value']
        print res['Failed']

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

    self.recursionLevel -= 1
    self.log.debug( "readFederation.__crawl: Current recursion level: %s" % self.recursionLevel )
    if self.recursionLevel == 0:
      self.__compareDictWithCatalog()
      try:
        os.remove('checkpoint.txt')
      except Exception, e:
        self.log.error("readFederation.__crawl: Failed to remove checkpoint")
      return S_OK( self.fileDict )

  def __writeCheckPoint( self ):
    self.log.debug("readFederation.__writeCheckPoint: Updating checkpoint")
    f = open('checkpoint.txt', 'w')
    for entry in self.history:
      f.write(entry+'\n')
    f.close()

  def __isFile( self, path ):
    self.log.debug("readFederation: Checking if %s is a file or not" % path)
    tries = 0
    while True and tries < self.max_tries:
      try:
        statInfo = self.gfal2.stat( path )

      except gfal2.GError, e:
        if e.code == errno.ENOENT:
          return S_ERROR( 'File does not exist' )
        else:
          tries += 1
          self.log.debug("readFederation: Failed to check file: (%s,%s), trying again." % (e.code, e.message))
          time.sleep(self.sleepTime)
      return S_OK( S_ISREG( statInfo.st_mode ) )

    return S_ERROR( "Couldn't check path, stopped trying after %s tries" % self.max_tries )

  def __compareDictWithCatalog( self ):
    """ Poll the filecatalog with the keys in self.fileDict and compare the catalog entries with the values of the fileDict.
    Once checked, remove the entry from the dictionary.
    :param self: self reference
    :return failed dict { 'NiC' : pfns not in catalog, 'NiS' : pfns in catalog but not storage}
    """
    self.log.debug("readFederation: gathered more than 40 file links: comparing with catalog now")
    failed = {}
    successful = {}
    dmScript = DMScript()
    fc = FileCatalog()
    SEDict = {}
    for urlList in self.fileDict.values():
      self.log.debug("readFederation: Retrieving LFN for %s" % urlList)
      lfn = dmScript.getLFNsFromList( urlList )
      if len(lfn):
        lfn = lfn[0]
        res = fc.getReplicas(lfn)
        if not res['OK']:
          res = res['Message']
          failed[lfn] = res
        else:
          res = res['Value']
          if lfn in res['Successful']:
            SEList = res['Successful'][lfn].keys()
            self.log.debug("readFederation.__compareDictWithCatalog: Retrieving TURL for each SE and check whether we have a match with the federation PFN")
            for SE in SEList:
              se = SEDict.get( SE, None )
              if not se:
                SEDict[SE] = StorageElement( SE, protocols='GFAL2_HTTP')
                se = SEDict[SE]
              res = se.getURL(lfn, protocol='http')
              if res['OK']:
                tURL = res['Value']['Successful'].values()
                # urlList holds all the urls that we need to check if they are also in the catalog so we compare if 
                # any of the url from urlList is the same
                for url in urlList:
                  if self.__compareURLS(tURL, url):
                    successful[lfn] = True
                    urlList.remove( url )
          # urls remaining in urlList are PFNs from the federation that couldn't be matched with any of the replica SEs
          # so the catalog doesn't know about them
        for url in urlList:
          failed[lfn] = url

          

    self.fileDict = {}

    return S_OK( { 'Successful' : successful, 'Failed' : failed } )

  def __compareURLS( self, fc_url, fed_url ):
    """ This method compares URLs, but should also consider, that maybe one URL doesn't have a port specified while the other has
    It is assumed that both URLs at least have protocol, host, path and filename defined.

    """
    self.log.debug("readFederation.__compareURLS: comparing TURL from SE with TURL from federation")
    fc_res = pfnparse(fc_url[0])['Value']
    fed_res = pfnparse(fed_url)['Value']
    key_list = ['Path', 'Filename', 'Port', 'Protocol', 'Host', 'WSUrl']
    isAMatch = True
    for key in key_list:
      fc_value = fc_res.get(key)
      fed_value = fed_res.get(key)
        # both keys have to exist and if their values are not the same then the url is not the same either
      if fc_value and fed_value:  
        if not fc_value == fed_value:
          isAMatch = False
          break

    return isAMatch



  def __readFile( self, afile ):
    """ Read the xml data from the file. Using gfal2.open to open file and read
        the content and write it into a string.

        :param self: self reference
        :param str filename: name of the metalink file to read
        :return str xml_string: a string containing the xml information of file
    """
    # open the file
    afile = afile+'?metalink'
    tries = 0
    successful = False
    while True and tries < self.max_tries:
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
    xml_string = f.read(10000)
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
  gLogger.setLevel("DEBUG")
  CA.initialize()
  CA.execute()
  #print CA._catalogAgent__readFile( 'http://federation.desy.de/fed/lhcb/data/2009/RAW/FULL/LHCb/BEAM1/62426/062426_0000000001.raw' )
  #print CA._catalogAgent__isFile( 'http://federation.desy.de/fed/lhcb/data/2009/RAW/FULL/LHCb/BEAM1/62426/' )


import sys
import os
import string
import xml.etree.ElementTree as ET
import gfal2
import errno
import time
from collections import deque

##### remove this part once it's integrated as agent #
from DIRAC.Core.Base.Script import parseCommandLine
from symbol import parameters
parseCommandLine()
######################################################


from DIRAC import gLogger, S_ERROR, S_OK
import pdb

from DIRAC.Resources.Catalog.FileCatalog       import FileCatalog
from DIRAC.Core.Base.AgentModule               import AgentModule

from stat import S_ISREG, S_ISDIR, S_IXUSR, S_IRUSR, S_IWUSR, \
  S_IRWXG, S_IRWXU, S_IRWXO

class catalogAgent( object ):

  def initialize( self ):
    """
    Setting up the crawler with a gfal2 context, a file catalog handle and an empty file dict
    :param self: self reference
    """
    self.gfal2  = gfal2.creat_context()
    self.rootURL = 'http://eoslhcb.cern.ch:8443/eos/lhcb/user/p/pgloor'
    self.fileDict = {}
    self.fc = FileCatalog()

    self.failedFiles = []
    self.failedDirectories = []
    self.failedEntries = []
    self.sleepTime = 4

  def execute( self ):
    """
    Run the crawler
    :param self: self reference
    """
    self.__crawl()


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

    directories = []

    tries = 0
    while True and tries < 10:
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
        break
      
      # if res['Value'] is true then it's a file  
      if res['Value']:
        res = self.__readFile( path )
        if not res['OK']:
          self.failedFiles[ {path : 'Failed to read xml data.'}]
        xml_string = res['Value']
        PFNs = self.__extractPFNs( xml_string )
        self.fileDict[path] = PFNs

      else:
        directories.append( path )

    if len(self.fileDict) > 40:
      self.__compareDictWithCatalog()

    for directory in directories:
      self.__crawl( directory )

    # add current path to finished list



  def __isFile( self, path ):
    tries = 0
    while True and tries < 10:
      try:
        statInfo = self.gfal2.stat( path )

      except gfal2.GError, e:
        if e.code == errno.ENOENT:
          return S_ERROR( 'File does not exist' )
        else:
          tries += 1
          time.sleep(self.sleepTime)

      return S_OK( S_ISREG( statInfo.st_mode ) )

    # if reading file wasn't successful we return the last error
    return res

  def __compareDictWithCatalog( self ):
    """ Poll the filecatalog with the keys in self.fileDict and compare the catalog entries with the values of the fileDict.
    Once checked, remove the entry from the dictionary.
    :param self: self reference
    :return failed dict { 'NiC' : pfns not in catalog, 'NiS' : pfns in catalog but not storage}
    """
    failed = {}
    successful = {}

    for afile, pfnlist in self.fileDict.items():
      res = fc.getReplicas( afile )
      if not res['OK']:
        pass

    pass


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
    while True and tries < 10:
      try:
        f = self.gfal2.open(afile, 'r')
        break
      except gfal2.GError, e:
        if e.code == errno.ENOENT:
          return S_ERROR( 'File does not exist' )
        else:
          tries += 1
          time.sleep(self.sleepTime)
    
    content = f.read(10000)
    xml_string = content
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
  CA.initialize()
  CA.execute()
  #print CA._catalogAgent__readFile( 'http://federation.desy.de/fed/lhcb/data/2009/RAW/FULL/LHCb/BEAM1/62426/062426_0000000001.raw' )
  #print CA._catalogAgent__isFile( 'http://federation.desy.de/fed/lhcb/data/2009/RAW/FULL/LHCb/BEAM1/62426/' )


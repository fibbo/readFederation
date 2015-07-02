import sys
import os
import string
import xml.etree.ElementTree as ET
import re
import gfal2

from DIRAC.Core.Base.Script import parseCommandLine
from symbol import parameters
parseCommandLine()

from DIRAC import gLogger, S_ERROR, S_OK
import pdb

from DIRAC.Resources.Catalog.FileCatalog       import FileCatalog
from DIRAC.Core.Base.AgentModule               import AgentModule

from stat import S_ISREG, S_ISDIR, S_IXUSR, S_IRUSR, S_IWUSR, \
  S_IRWXG, S_IRWXU, S_IRWXO

class catalogAgent( object ):

  def initialize( self ):
    self.gfal2  = gfal2.creat_context()
    self.rootURL = 'http://federation.desy.de/fed/'
    self.fileDict = {}
    self.fc = FileCatalog()

  def execute( self ):
    self.__crawl( self.rootURL )

  def __crawl( self, basepath ):
    directories = []
    files = []
    entries = self.gfal2.listdir( basepath )

    for entry in entries:
      path = '/'.join( [basepath, entry] )
      if isFile( path ):
        files.append( path )
      else:
        directories.append( path )

    for afile in files:
      xml_string = self.__readFile( afile )
      PFNs = self.__extractPFNs( xml_string )
      self.fileDict[afile] = PFNs

    if len(self.fileDict) > 40:
      self.__compareDictWithCatalog()

    for directory in directories:
      self.__crawl( directory )

    # add current path to finished list




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


  def __lookForDarkData( self ):
    testfile = 'http://federation.desy.de/fed/lhcb/LHCb/Collision12/BHADRON.MDST/00030613/0000/00030613_00000001_1.bhadron.mdst?metalink'
    xml_string = self.__readFile( testfile )
    PFNs = self.__extractPFNs( xml_string )
    print PFNs
      
  def __readFile( self, file ):
    """ Read the xml data from the file. Using gfal2.open to open file and read
        the content and write it into a string.

        :param self: self reference
        :param str filename: name of the metalink file to read
        :return str xml_string: a string containing the xml information of file
    """
    # open the file
    file = file+'?metalink'
    f = self.gfal2.open(file, 'r')
    # read first 10 bytesthe max_size first bytes.
    
    # Hex dump
    xml_string = []
    while True:
      content = f.read(200)
      if not content:
        break
      for byte in content:
        if byte in string.printable:
          xml_string.append(byte)
        else:
          xml_string.append('.')
    xml_string = ''.join(xml_string)
    return xml_string


  def __extractPFNs( self, xml_string ):
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


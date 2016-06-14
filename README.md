# Filecatalog crawler
#### General

This crawler uses the desy federation website (http://federation.desy.de/fed/lhcb/) as a base to compare the filecatalog with the data saved on the storage elements. If a file is shown on the federation website to be available on a specific SE the crawler will check if the catalog agrees. If not the catalog is missing information. However the crawler only cares that files that are on the federation are also in the crawler, but not vice versa.
For now the crawler is a standalone agent. Proper DIRAC agent behaviour has to be implemented in order to work within the DIRAC framework.

#### Usage

To start the agent initialize the catalog agent and initialize.
```
CA = catalogAgent()
  gLogger.setLevel("NOTICE")
  res = CA.initialize()
```

Following two parameters are in the initializer that can be changed:

```
self.rootURL = 'http://federation.desy.de/fed/lhcb/LHCb/Collision10'
self.dedicatedSE = [] #['CNAF_M-DST','IN2P3_M-DST','CERN-USER']
```

self.rootURL: Starting point of the crawl. Once the rootURL is reached the crawl has finished
self.dedicatedSE: Specify a list of storage elements (SE) which the crawler should compare to. If we check only for SE X and Ia file is shown on the federation website to be available on SE X but the catalog only shows Y and Z then this file will be shown as missing on X. If the federation shows files to be on Y and Z it doesn't check whether
they are in the catalogs under Y and Z.


#### To be done

For now the crawler saves the results in a pickle file and this only after it has finished crawling. This should be improved that the crawl will be dumped in specific intervalls (i.e. after ˜5000 files have been checked). After x files checked it should either:

* read the pickle dump into a dictionary
* update the dictionary with the new results
* dump the results

or

* dump the dictionary with the x files result in a new pickle file

Method could suffer from long dumping times because it rereads the pickle dump (you cannot add to an existing pickle). Method two will possibly create many pickle files. A database approach is also possible.
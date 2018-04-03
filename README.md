UK-WEB-ARCHIVE Semantic Change
=================================

Code for the project on semantic change detection using the UK Web Archive

Install
----------

* Clone the git repository
* Run the command 'mvn package'
* Rename the file 'config.ex.properties' in 'config.properties'
* Setup the 'config.properties' file, useful comments are in the file
* Use the script start.sh for running main classes or build your command line. JAR files are in the 'target' subfolder, while JAR libraries are in the 'target/lib' subfolder

Build the application for the Azure Batch
--------------------------------------------

In order to build the application for the VMs in the Azure pool, you must create a zip file containing:

* the JAR of the application. This JAR is created in the target folder after the command 'mvn package'. You must add the file that has the substring 'jar-with-dependencies' in the file name
* the file config.properties
* the file run.sh
* the file contentType.filter

More details about how to upload the application on Azure are here: https://docs.microsoft.com/en-us/azure/batch/batch-application-packages

Details about how to build a batch account on Azure are here: https://github.com/alan-turing-institute/azure-batch-tools/blob/master/docs/az-vm-pool-management.md

Main classes (for VMs pool)
------------------------------
* *ati.ukwebarchive.azure.WetBatchMulti*: this class processes all WARC and ARC files in the main container specified in the config file. For each block, it extracts records that match valid mime-types and stores the textual content in compressed WET files.
* *ati.ukwebarchive.azure.tokenize.TokenBatchMulti*: this class processes all WET files in the main container specified in the config file. For each block, the text is tokenized and saved on a GZIP compressed file on the store container.

Main classes for processing without VMs pool
-----------------------------------------------
* *ati.ukwebarchive.azure.tokenize.BlobDir2WetProcessor*: this class processes all blobs (arc or warc) that belong to a particular prefix in a storage container.
* *ati.ukwebarchive.azure.tokenize.BlobDir2WetProcessorRecursive*: this class is *EXPERIMENTAL*. The class processes recursively all blobs (arc or warc) that belong to a particular prefix in a storage container.
* *ati.ukwebarchive.azure.tokenize.BlobDir2TokenProcessor*: this class processes (tokenizes) all blobs (wet) that belong to a particular prefix in a storage container.

Statistics about the first extraction process (March 2018)
-------------------------------------------------------------
* 447,197 ARC/WARC blobs in the dataset 1996-2010
* 67,834 ARC/WARC blobs in the dataset 2011-2013
* WET extraction process: 445,580 (99.64%) blobs for 1996-2010 and 67,830 (99.99%) for 2011-2013. Some blobs were not processed due to errors. Some errors occur for corrupted ARC/WARC files, sometimes the JWAT library recognizes ARC/WARC files as corrupted. In the first part of the processing, the Apache Tika library reported a lot of errors. Tika was replaced by the Jsoup library.
* The extraction process produces about 3,01 Terabytes of WET files.

For other details see the *javadoc* and *comments* in source files.

This code is *under development* and *experimental*, for any doubts please contact pierpaolo.basile@gmail.com.

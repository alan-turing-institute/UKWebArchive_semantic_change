UK-WEB-ARCHIVE Semantic Change
=================================

Code for the project on semantic change detection using the UK Web Archive

Install
----------

* Clone the git repository
* Run the command 'mvn package'
* Rename the file 'config.ex.properties' in 'config.properties'
* Setup the 'config.properties' file, some comments are in the file
* Use the script run.sh for running main classes or build your command line. JAR files are in the 'target' subfolder, while JAR libraries are in the 'target/lib' subfolder

Main classes
---------------
* ati.ukwebarchive.content.ContentExtractorMT: this class processes all WARC and ARC files in the main container specified in the config file. For each block, it extracts records that match valid mime-types and stores the textual content in compressed GZIP files. A file for each month+year is created.
* ati.ukwebarchive.cdx.CdxStatisticsMT: this class processes all CDX files in the main container specified in the config file. For each CDX block, it extracts statistics about how many records match valid mime-types and stores the results in a TSV file.

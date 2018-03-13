# wikiDumpParser

Simple Python3 script for parsing the Wikipedia XML Data Dump and extracting all link and category information. Parsed Data needs some cleanup.

Currently works with english language Wikipedia and 7z compressed files. 

## Prerequisites
1. Make sure all required python libraries are installed as well as p7zip on your host system.
2. Create a list of dump files to parse from https://dumps.wikimedia.org/backup-index.html (As of now only 7z zipped enwiki files work.

## How to use

Import library into your project (You might need to place the wikiDumpParser folder in the directory of your Python script):

```
from wikiDumpParser.wikiDumpParser import *
```

Create project:

```
project = Project()
project.create_project(start_date='2016-11-01')
```

Passing the start date is optional. If not set, the entire dump will be parsed. If set, only revisions newer than the sart date are used.

Loading an existing project:

```
project = Project()
project.load_project()
```

Adding dump files to be parsed:

```
dump_list = 'dump_list.csv'
base_url = 'https://dumps.wikimedia.org/enwiki/20180301/'
project.add_dump_file_info(dump_list, base_url)
```

Processing the dump:
```
project.process()
```

In case the processing is interrupted because the script fails, it can be resumed. However, you might need to clean up the last step beforehand, i.e. remove partially downladed file from the data folder or remove partially parsed results.
Processing is done in four steps:
1. Download the file
2. Unpack and split the xml file in individual pages
3. Parse pages
4. Postprocessing of results (some cleanup, but requires more)

The wikiDumpParser supports parallel processing. The default number of parallel processes is 1, but can be changed.

```
project.set_parallel_processes(number)
```

The parsed results are all but perfect and need some more postprocessing. Be aware, parsing the entire Wikipedia XML Dump is very time and resource consuming.


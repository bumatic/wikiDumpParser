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
dump_status = 'dumpstatus.json'
url_base = 'https://dumps.wikimedia.org/enwiki/20180301/'

project = Project()
project.create_project(start_date='2016-11-01')
project.load_dump_info(dump_status, url_base)
project.set_parallel_processes(10)
```

Passing the start date is optional. If not set, the entire dump will be parsed. If set, only revisions newer than the sart date are used.

Loading an existing project:

```
project = Project()
project.load_project()
```

Processing the dump:
```
project.process()
```

In case the processing is interrupted because the script fails, it can be resumed. Partially processed files will be rolled back. However, these partial results might already have been added to the page_info file and the revisions file. Before using them later, duplicated need to be removed from them. 

Processing is done in four steps:
1. Download the file
2. Unpack and split the xml file in individual pages
3. Parse pages
4. Postprocessing of results (some cleanup, but requires more)

The wikiDumpParser supports parallel processing. The default number of parallel processes is 1, but can be changed.

```
project.set_parallel_processes(number)
```

It is recommended to run the parser in a terminal screen. Checking the current status can be done by running the following script in another terminal:

```
from wikiDumpParser.wikiDumpParser import *
project = Project()
project.load_project()
project.get_processing_status()
```

The parsed results are all but perfect and need some more postprocessing. Be aware, parsing the entire Wikipedia XML Dump is very time and resource consuming. Parsing the dump partially  (2016-11-01 until 2018-03-01) took about 6 days with 10 parallel proccess running on SSD storage. 

Once the parser is done the results need some further processing:

```
project.process_results()
```

The resulting files can be combined with existing data by running:
```
project.combine_old_and_new(path='old',
                            cats='old_cats.csv',
                            links=[old_links ...]
                            page_info='old_page_info.csv',
                            revisions='old_revisions.csv')
```
Currently combining links is not implemented
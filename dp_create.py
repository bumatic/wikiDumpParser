from wikiDumpParser.wikiDumpParser import *

dump_status = 'dumpstatus.json'
url_base = 'https://dumps.wikimedia.org/enwiki/20180301/'

project = Project()
project.create_project(start_date='2016-11-01')
project.load_dump_info(dump_status, url_base)
project.set_parallel_processes(5)
project.get_processing_status()



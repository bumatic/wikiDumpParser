from wikiDumpParser.wikiDumpParser import *

dump_status = 'dumpstatus.json'
url_base = 'https://dumps.wikimedia.org/enwiki/20180720/'

project = Project()
project.create_project(start_date='1990-01-01')
project.load_dump_info(dump_status, url_base)
project.set_parallel_processes(10)
project.get_processing_status()



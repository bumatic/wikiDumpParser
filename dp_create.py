from wikiDumpParser.wikiDumpParser import *

dump_status = 'dumpstatus.json'
url_base = 'https://dumps.wikimedia.org/enwiki/20180401/'

project = Project()
project.create_project()
project.load_dump_info(dump_status, url_base)
project.set_parallel_processes(10)
project.get_processing_status()



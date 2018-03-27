from wikiDumpParser.wikiDumpParser import *

dump_list = 'enwiki-20180301_dump_list.csv'
url_base = 'https://dumps.wikimedia.org/enwiki/20180301/'




project = Project()
project.create_project(start_date='2016-11-01')
project.add_dump_file_info(dump_list, url_base)
project.set_parallel_processes(10)
#project.load_project()
project.get_processing_status()
project.process()



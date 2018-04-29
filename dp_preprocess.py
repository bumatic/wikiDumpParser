from wikiDumpParser.wikiDumpParser import *

project = Project()
project.load_project()
project.set_parallel_processes(10)
project.get_templates()
#project.update_status()
#project.retry_errors()

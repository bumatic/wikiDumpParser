from wikiDumpParser.wikiDumpParser import *

project = Project()
project.load_project()
project.process()
#project.update_status()
#project.retry_errors()

from wikiDumpParser.wikiDumpParser import *

project = Project(path='test_data')
project.load_project()
project.get_processing_status()
project.process()

#project.retry_errors()

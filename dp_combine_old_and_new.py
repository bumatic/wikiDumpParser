from wikiDumpParser.wikiDumpParser import *
project = Project()
project.load_project()
project.combine_old_and_new(path='old',
                            cats='old_cats.csv',
                            page_info='old_page_info.csv',
                            revisions='old_revisions.csv')

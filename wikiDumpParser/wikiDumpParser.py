import os
import json
import pandas as pd
import shutil
import logging
from dateutil import parser

from datetime import datetime
from wikiDumpParser.preprocessorData import *
from wikiDumpParser.processorData import *
from wikiDumpParser.processorResults import *
from joblib import Parallel, delayed

# ToDo: Implement Parsing of Templates and associated Categories
# ToDo: Implement Parsing of which templates are assigned to
class Project:
    def __init__(self, path='project'):
        self.path = path
        self.data_path = os.path.join(self.path, 'data')
        self.results_path = os.path.join(self.data_path, 'results')
        self.templates_path = os.path.join(self.data_path, 'templates')
        self.tmp_status_path = os.path.join(self.data_path, 'tmp_status')
        self.pinfo = {}
        self.pinfo_file = os.path.join(self.path, '_project_info.json')
        self.pinfo['start_date'] = parser.parse('1990-01-01').timestamp()
        self.pinfo['parallel_processes'] = 1
        self.pinfo['templates'] = False
        self.pinfo['logging'] = {}
        self.pinfo['logging']['quiet'] = False
        self.pinfo['logging']['debug'] = False
        self.logger = self.createLogger(self.pinfo['logging']['quiet'], self.pinfo['logging']['debug'])

    def create_project(self, start_date=None, dump_date=None):
        if not os.path.isdir(os.path.join(os.getcwd(), self.path)):
            os.makedirs(os.path.join(os.getcwd(), self.path))
        if not os.path.isdir(os.path.join(os.getcwd(), self.data_path)):
            os.makedirs(os.path.join(os.getcwd(), self.data_path))
        if not os.path.isdir(os.path.join(os.getcwd(), self.tmp_status_path)):
            os.makedirs(os.path.join(os.getcwd(), self.tmp_status_path))
        if not os.path.isdir(os.path.join(os.getcwd(), self.templates_path)):
            os.makedirs(os.path.join(os.getcwd(), self.templates_path))
        if start_date is not None:
            self.pinfo['start_date'] = parser.parse(start_date).timestamp()
        if dump_date is not None:
            self.pinfo['dump_date'] = parser.parse(dump_date).timestamp()
        if os.path.exists(self.pinfo_file):
            self.logger.info("A project already exists in '%s'. Try loading this project or "
                             "change location for new project.", self.path)
        else:
            self.save_project()

    def load_project(self):
        if os.path.exists(os.path.join(os.getcwd(), self.pinfo_file)):
            with open(os.path.join(os.getcwd(), self.pinfo_file), 'r') as info_file:
                self.pinfo = json.load(info_file)
            info_file.close()
        self.update_status()
        self.set_logging_level(self.pinfo['logging']['quiet'], self.pinfo['logging']['quiet'])

        #self.logger = self.createLogger(self.pinfo['logging']['quiet'], self.pinfo['logging']['quiet'])

    def save_project(self):
        with open(self.pinfo_file, 'w') as info_file:
            json.dump(self.pinfo, info_file, sort_keys=True, indent=4)
        return

    def save_tmp_status(self, filename, status):
        with open(os.path.join(self.tmp_status_path, filename), 'w') as info_file:
            json.dump(status, info_file, sort_keys=True, indent=4)
        return

    def update_status(self):
        for f in glob.glob(self.tmp_status_path + '/*'):
            with open(os.path.join(os.getcwd(), f), 'r') as info_file:
                status = json.load(info_file)
            self.pinfo['dump'][os.path.basename(f)+'.7z'] = status
            self.save_project()
            os.remove(f)

    def set_start_date(self, date):
        self.pinfo['start_date'] = parser.parse(date)
        self.save_project()

    def get_start_date(self):
        if 'start_date' in self.pinfo.keys():
            return datetime.fromtimestamp(self.pinfo['start_date']).strftime('%Y-%m-%d')
        else:
            print('No start date has been set')
            return None

    def set_dump_date(self, date):
        self.pinfo['dump_date'] = parser.parse(date)
        self.save_project()

    def get_dump_date(self):
        if 'dump_date' in self.pinfo.keys():
            return datetime.fromtimestamp(self.pinfo['dump_date']).strftime('%Y-%m-%d')
        else:
            print('No dump date has been set.')
            return None

    def set_parallel_processes(self, number):
        assert type(number) is int, "Number of parallel processes is not an integer."
        self.pinfo['parallel_processes'] = number
        self.save_project()

    def get_parallel_processes(self):
        if 'parallel_processes' in self.pinfo.keys():
            return self.pinfo['parallel_processes']
        else:
            print('No number of parallel processes has been set.')
            return None

    def add_dump_file_info(self, file_list, base_url):
        if base_url[-1:] != '/':
            base_url = base_url+'/'
        self.pinfo['base_url'] = base_url
        file_list = pd.read_csv(file_list, header=None, delimiter='  ', names=['md5', 'name'])
        if 'dump' not in self.pinfo.keys():
            self.pinfo['dump'] = {}
            self.pinfo['md5'] = {}

            for f in file_list.iterrows():
                self.pinfo['dump'][f[1]['name']] = 'init'
                self.pinfo['md5'][f[1]['name']] = f[1]['md5']
            self.save_project()
        else:
            print('Dump list has already been added to project.')

    def load_dump_info(self, json_file, base_url):
        if base_url[-1:] != '/':
            base_url = base_url+'/'
        self.pinfo['base_url'] = base_url
        url = base_url + json_file
        dump_info_file = 'dump_info.json'
        response = requests.get(url, stream=True)
        with open(os.path.join(self.path, dump_info_file), "wb") as handle:
            for data in response.iter_content(chunk_size=32768):
                handle.write(data)
        with open(os.path.join(os.getcwd(), self.path, dump_info_file), 'r') as info_file:
            dump_info = json.load(info_file)
        info_file.close()

        self.pinfo['dump'] = {}
        self.pinfo['md5'] = {}

        for key, value in dump_info['jobs']['metahistory7zdump']['files'].items():
            if bool(value):
                self.pinfo['dump'][key] = 'init'
                self.pinfo['md5'][key] = value['md5']
        self.save_project()
        os.remove(os.path.join(os.getcwd(), self.path, dump_info_file))

    def get_processing_status(self):
        self.update_status()
        if 'dump' in self.pinfo.keys():
            total = 0
            init = 0
            downloaded = 0
            download_started = 0
            split = 0
            splitting_started = 0
            parsed = 0
            parsing_started = 0
            post = 0
            postprocessing_started = 0
            error = 0
            done = 0
            for item, value in self.pinfo['dump'].items():
                total += 1
                if value == 'init':
                    init += 1
                elif value == 'downloaded':
                    downloaded += 1
                elif value == 'download_started':
                    download_started += 1
                elif value == 'split':
                    split += 1
                elif value == 'splitting_started':
                    splitting_started += 1
                elif value == 'parsed':
                    parsed += 1
                elif value == 'parsing_started':
                    parsing_started += 1
                elif value == 'post':
                    post += 1
                elif value == 'postprocessing_started':
                    postprocessing_started += 1
                elif value == 'done':
                    done += 1
                elif value == 'error':
                    error += 1
            print('===================================================')
            print('Total number of files to process: '+str(total))
            print('Number of files done: ' + str(done))
            print('Number of files post-processed: ' + str(post))
            print('Number of files post-processing started: ' + str(postprocessing_started))
            print('Number of files parsed: ' + str(parsed))
            print('Number of files parsing_started: ' + str(parsing_started))
            print('Number of files split: ' + str(split))
            print('Number of files splitting started: ' + str(splitting_started))
            print('Number of files downloaded: ' + str(downloaded))
            print('Number of files download started: ' + str(download_started))
            print('Number of files with errors: ' + str(error))
            print('Number of files not yet started: ' + str(init))
        else:
            print("No dump files have been added yet for processing.")
            return

    def cleanup(self):
        for key, value in self.pinfo['dump'].items():
            if value in ['preprocessing_started','download_started', 'splitting_started', 'parsed_started', 'postprocessing_started']:
                # remove downloaded file: rest to error
                try:
                    shutil.rmtree(os.path.join(self.data_path, key[:-3]))
                except:
                    pass
                try:
                    shutil.rmtree(os.path.join(self.data_path, 'results', key[:-3]))
                except:
                    pass
                self.pinfo['dump'][key] = 'error'
                self.save_project()

    def retry_errors(self):
        self.cleanup()
        for key, value in self.pinfo['dump'].items():
            if value == 'error':
                self.pinfo['dump'][key] = 'init'
                self.save_project()


    def process(self):
        '''
        process_order = ['done',
                         'post',
                         'parsed',
                         'split',
                         'downloaded',
                         'init']
        '''

        if 'dump' not in self.pinfo.keys():
            print('No dump file info has been added to the project yet – use: '
                  'Project.add_dump_file_info(self, file_list, base_url)')
        else:
            self.update_status()
            self.cleanup()
            Parallel(n_jobs=self.pinfo['parallel_processes'])\
                (delayed(self.process_file)(f, status) for f, status in self.pinfo['dump'].items())

    def process_file(self, f, status):
        while status != 'post':
            if status == 'error':
                return
            self.logger.info("%s: %s. Go to next step.", status, f)
            print('Call next Processor for ' + status + ' file: ' + f)
            if status == 'init':
                tmp_status = 'download_started'
            if status == 'downloaded' or 'preprocessed':
                tmp_status = 'splitting_started'
                # os.path.join(self.data_path, f[:-3])
            if status == 'split':
                tmp_status = 'parsing_started'
            if status == 'parsed':
                tmp_status = 'postprocessing_started'

            self.save_tmp_status(f[:-3], tmp_status)

            status = Processor(f, self.data_path, self.pinfo['base_url'], status, self.pinfo['start_date'],
                               self.pinfo['md5'][f]).process()
            self.save_tmp_status(f[:-3], status)

    def process_results(self):
        ProcessorResults(self).process()

    def combine_old_and_new(self, path=None, cats=None, links=None, page_info=None, revisions=None):
        ProcessorResults(self).combine_old_and_new(path=path, cats=cats, links=links,
                                                   page_info=page_info, revisions=revisions)

    def get_templates(self):
        if 'dump' not in self.pinfo.keys():
            print('No dump file info has been added to the project yet – use: '
                  'Project.add_dump_file_info(self, file_list, base_url)')
            return
        if self.pinfo['templates']:
            print('Template files have already been assembled.')
            return

        self.update_status()
        self.cleanup()
        Parallel(n_jobs=self.pinfo['parallel_processes'])\
            (delayed(self.preprocess_file)(f, status) for f, status in self.pinfo['dump'].items())

    def preprocess_file(self, f, status):
        while status != 'preprocessed':
            if status == 'error':
                return
            self.logger.info("%s: %s. Go to next step.", status, f)
            if status == 'init':
                tmp_status = 'download_started'
            if status == 'downloaded':
                tmp_status = 'preprocessing_started'
            self.save_tmp_status(f[:-3], tmp_status)
            status = PreProcessor(f, self.data_path, self.pinfo['base_url'], status, self.pinfo['start_date'],
                               self.pinfo['md5'][f], self.logger).preprocess()
            self.save_tmp_status(f[:-3], status)

    def createLogger(self, quiet, debug):
        logger = logging.getLogger()
        if not quiet:
            logger.setLevel(logging.INFO)
        if debug:
            logger.setLevel(logging.DEBUG)

        print(logger.info())
        return logger

    def set_logging_level(self, quiet=False, debug=False):
        self.pinfo['logging']['quiet'] = False
        self.pinfo['logging']['debug'] = False
        self.save_project()
        if not quiet:
            self.logger.setLevel(logging.INFO)
        if debug:
            self.logger.setLevel(logging.DEBUG)
        self.logger.info("Logging level has been set to quiet == '%s' and debug == '%s'", quiet, debug)



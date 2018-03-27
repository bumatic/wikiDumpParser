import os
import json
import pandas as pd
from dateutil import parser
from datetime import datetime
from wikiDumpParser.processor import *
from joblib import Parallel, delayed


class Project:
    def __init__(self):
        self.path = 'project'
        self.data_path = os.path.join(self.path, 'data')
        self.pinfo = {}
        self.pinfo_file = os.path.join(self.path, '_project_info.json')
        self.pinfo['start_date'] = parser.parse('1990-01-01').timestamp()
        self.pinfo['parallel_processes'] = 1

    def create_project(self, start_date=None, dump_date=None):
        if not os.path.isdir(os.path.join(os.getcwd(), self.path)):
            os.makedirs(os.path.join(os.getcwd(), self.path))
        if not os.path.isdir(os.path.join(os.getcwd(), self.data_path)):
            os.makedirs(os.path.join(os.getcwd(), self.data_path))
        if start_date is not None:
            self.pinfo['start_date'] = parser.parse(start_date).timestamp()
        if dump_date is not None:
            self.pinfo['dump_date'] = parser.parse(dump_date).timestamp()

        if os.path.exists(self.pinfo_file):
            print('A project already exists in this location. Try loading or change location for new project.')
        else:
            self.save_project()

    def load_project(self):
        if os.path.exists(os.path.join(os.getcwd(), self.pinfo_file)):
            with open(os.path.join(os.getcwd(), self.pinfo_file), 'r') as info_file:
                self.pinfo = json.load(info_file)
            info_file.close()

    def save_project(self):
        with open(self.pinfo_file, 'w') as info_file:
            json.dump(self.pinfo, info_file, sort_keys=True, indent=4)
        return

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
        if 'dump' in self.pinfo.keys():
            total = 0
            init = 0
            downloaded = 0
            split = 0
            parsed = 0
            post = 0
            done = 0
            for item, value in self.pinfo['dump'].items():
                total += 1
                if value == 'init':
                    init += 1
                elif value == 'downloaded':
                    downloaded += 1
                elif value == 'split':
                    split += 1
                elif value == 'parsed':
                    parsed += 1
                elif value == 'post':
                    post += 1
                elif value == 'done':
                    done += 1
            print('Total number of files to process: '+str(total))
            print('Number of files done: ' + str(done))
            print('Number of files post-processed: ' + str(post))
            print('Number of files parsed: ' + str(parsed))
            print('Number of files split: ' + str(split))
            print('Number of files downloaded: ' + str(downloaded))
            print('Number of files not yet started: ' + str(init))
        else:
            print("No dump files have been added yet for processing.")
            return


    '''
    def download_all_first(self):
        if 'dump' not in self.pinfo.keys():
            print('No dump file info has been added to the project yet – use: '
                  'Project.add_dump_file_info(self, file_list, base_url)')
        else: 
            for f, status in self.pinfo['dump'].items():
                if status == 'init':
                    status = Processor(f, self.data_path, self.pinfo['base_url'], status, self.pinfo['start_date']).process()
                    self.pinfo['dump'][f] = status
                    self.save_project()
                    #self.process_file(f, status, 'init')
                    print(self.get_processing_status())
    '''


    def process(self):
        process_order = ['done',
                         'post',
                         'parsed',
                         'split',
                         'downloaded',
                         'init']

        if 'dump' not in self.pinfo.keys():
            print('No dump file info has been added to the project yet – use: '
                  'Project.add_dump_file_info(self, file_list, base_url)')

        else:
            for step in process_order:
                Parallel(n_jobs=self.pinfo['parallel_processes'])(delayed(self.process_file)(f, status, step)
                                   for f, status in self.pinfo['dump'].items())

    def process_file(self, f, status, step):
        while status != 'post':
            print('Call next Processor for ' + status + ' file: ' + f)
            status = Processor(f, self.data_path, self.pinfo['base_url'], status, self.pinfo['start_date'],
                               self.pinfo['md5'][f]).process()
            self.pinfo['dump'][f] = status
            self.save_project()

import os
import json
import pandas as pd
from dateutil import parser
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

    def set_dump_date(self, date):
        self.pinfo['dump_date'] = parser.parse(date)
        self.save_project()

    def set_parallel_processes(self, number):
        assert type(number) is int, "Number of parallel processes is not an integer."
        self.pinfo['parallel_processes'] = number

    def add_dump_file_info(self, file_list, base_url):
        self.pinfo['base_url'] = base_url
        file_list = pd.read_csv(file_list, header=None, delimiter='\t', names=['name'])
        if 'dump' not in self.pinfo.keys():
            self.pinfo['dump'] = {}
            for file in file_list.iterrows():
                self.pinfo['dump'][file[1]['name']] = 'init'
            self.save_project()
        else:
            print('Dump list has already been added to project.')

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
                Parallel(n_jobs=self.pinfo['parallel_processes'])(delayed(self.process_file)(file, status, step)
                                   for file, status in self.pinfo['dump'].items())

    def process_file(self, file, status, step):
        while status != 'post':
            print('Call next Processor for ' + status + ' file: ' + file)
            status = Processor(file, self.data_path, self.pinfo['base_url'], status, self.pinfo['start_date']).process()
            self.pinfo['dump'][file] = status
            self.save_project()

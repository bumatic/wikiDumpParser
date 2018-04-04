import os
from pyunpack import Archive
import shutil
import pandas as pd
from tqdm import tqdm


class ProcessorResults:
    def __init__(self, project):
        self.project = project

    @staticmethod
    def unpack(path, f):
        Archive(os.path.join(path, f)).extractall(os.path.join(os.getcwd(), path))
        return f[:-3]

    def process(self):
        self.assemble_cat_results()
        self.update_revisions_file()
        self.group_links_files()
        self.group_page_info()

    def assemble_cat_results(self):
        for key, value in tqdm(self.project.pinfo['dump'].items(), desc='Assemble category results in one file:'):
            path = os.path.join(self.project.results_path, key[:-3])
            f = 'cats.csv.7z'
            f = self.unpack(path, f)
            data = pd.read_csv(os.path.join(path, f), header=None, delimiter='\t', na_filter=False)
            # self.cats = self.cats.append(data)
            results = os.path.join(self.project.data_path, 'cats_all.csv')
            data.to_csv(results, sep='\t', index=False, header=False, mode='a')
            os.remove(os.path.join(path, f))

    def update_revisions_file(self):
        relevant_revs_file = os.path.join(self.project.results_path, 'relevant_revisions.csv')
        rev_data_file = os.path.join(self.project.results_path, 'revisions.csv')
        relevant_revs = pd.read_csv(relevant_revs_file, delimiter='\t', names=['rev_id'])
        rev_data = pd.read_csv(rev_data_file, delimiter='\t', names=['page_id', 'rev_id', 'ts'])
        rev_data = rev_data[rev_data['rev_id'].isin(relevant_revs['rev_id'])].reset_index().drop('index', 1)
        results = os.path.join(self.project.data_path, 'revisions_processed.csv')
        rev_data.to_csv(results, sep='\t', index=False, header=False, mode='w')

    def group_links_files(self):
        results_path = os.path.join(self.project.data_path, 'links_all')
        if not os.path.isdir(results_path):
            os.makedirs(results_path)
        for key, value in tqdm(self.project.pinfo['dump'].items(), desc='Group Link Results:'):
            path = os.path.join(self.project.results_path, key[:-3])
            f = 'links.csv.7z'
            source = os.path.join(path, f)
            destination = os.path.join(results_path, key[:-3] + '_' + f)
            shutil.copy2(source, destination)

    def group_page_info(self):
        shutil.copy2(os.path.join(self.project.results_path, 'page_info.csv'),
                     os.path.join(self.project.data_path, 'page_info.csv'))

    def combine_old_and_new(self, path=None, cats=None, links=None, page_info=None, revisions=None):
        if path is None:
            path = '/'
        if cats is not None:
            old_file = os.path.join(self.project.data_path, path, cats)
            new_file = os.path.join(self.project.data_path, 'cats_all.csv')
            results_file = os.path.join(self.project.data_path, 'cats_combined.csv')
            results = pd.DataFrame()
            if os.path.isfile(old_file) and os.path.isfile(new_file):
                results = results.append(pd.read_csv(old_file, delimiter='\t', na_filter=False))
                results = results.append(pd.read_csv(new_file, delimiter='\t', na_filter=False))
                results.to_csv(results_file, sep='\t', index=False, header=False, mode='w')
                del results
            else:
                print('New or old categories file does not exist in the expected location')
        if revisions is not None:
            old_file = os.path.join(self.project.data_path, path, revisions)
            new_file = os.path.join(self.project.data_path, 'revisions_processed.csv')
            results_file = os.path.join(self.project.data_path, 'revisions_combined.csv')
            results = pd.DataFrame()
            if os.path.isfile(old_file) and os.path.isfile(new_file):
                results = results.append(pd.read_csv(old_file, delimiter='\t', na_filter=False))
                results = results.append(pd.read_csv(new_file, delimiter='\t', na_filter=False))
                results = results.drop_duplicates()
                results.to_csv(results_file, sep='\t', index=False, header=False, mode='w')
                del results
            else:
                print('New or old revisions file does not exist in the expected location')
        if page_info is not None:
            old_file = os.path.join(self.project.data_path, path, page_info)
            new_file = os.path.join(self.project.data_path, 'page_info.csv')
            results_file = os.path.join(self.project.data_path, 'page_info_combined.csv')
            results = pd.DataFrame()
            if os.path.isfile(old_file) and os.path.isfile(new_file):
                results = results.append(pd.read_csv(old_file, delimiter='\t', na_filter=False))
                results = results.append(pd.read_csv(new_file, delimiter='\t', na_filter=False))
                results = results.drop_duplicates()
                results.to_csv(results_file, sep='\t', index=False, header=False, mode='w')
                del results
            else:
                print('New or old revisions file does not exist in the expected location')
        if links is not None:
            print('HANDLING of Links is not yet implemented')
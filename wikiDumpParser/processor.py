import os
from pyunpack import Archive
from tqdm import tqdm
import requests
from retrying import retry
from xml.sax import parse
from xml.sax.saxutils import XMLGenerator
import numpy as np
from lxml import etree
import re
import shutil
import glob
import subprocess
from dateutil import parser
import pandas as pd
import hashlib
import random
import time


class Processor:
    def __init__(self, file_name, data_path, base_url, status, start_date, md5):
        self.file_name = file_name
        self.data_path_base = data_path
        self.data_path = os.path.join(self.data_path_base, os.path.splitext(self.file_name)[0])
        if not os.path.isdir(os.path.join(os.getcwd(), self.data_path)):
            os.makedirs(os.path.join(os.getcwd(), self.data_path))
        self.base_url = base_url
        self.status = status
        self.start_date = start_date
        self.md5 = md5

    def process(self):
        if self.status == 'init':
            success = self.download_dump_file()
            if success:
                new_status = 'downloaded'
                return new_status
            else:
                new_status = 'init'
                print('Download error. Waiting 60 to 120 seconds to restart.')
                time.sleep(random.randint(60, 120))
                return new_status
        if self.status == 'downloaded':
            # ONCE IMPLEMENTE NEW_STATUS NEEDS TO BE SET TO NEXT.
            # While not everything is IMPLEMENTED THIS LEADS TO INFINITE LOOP
            success = self.split()
            if success:
                new_status = 'split'
                return new_status
        if self.status == 'split':
            # ONCE IMPLEMENTE NEW_STATUS NEEDS TO BE SET TO NEXT.
            # While not everything is IMPLEMENTED THIS LEADS TO INFINITE LOOP
            success = self.parse()
            if success:
                new_status = 'parsed'
                return new_status
        if self.status == 'parsed':
            # ONCE IMPLEMENTE NEW_STATUS NEEDS TO BE SET TO NEXT.
            # While not everything is IMPLEMENTED THIS LEADS TO INFINITE LOOP
            success = self.postprocessing_cat_link()
            if success:
                new_status = 'post'
                return new_status

    @retry(wait_random_min=1000, wait_random_max=20000, stop_max_attempt_number=20)
    def download_dump_file(self):
        x = random.randint(1, 120)
        time.sleep(x)
        response = requests.get(self.base_url + self.file_name, stream=True)
        with open(os.path.join(self.data_path, self.file_name), "wb") as handle:
            for data in response.iter_content(chunk_size=32768):
                handle.write(data)
        new_md5 = self.get_md5(os.path.join(self.data_path, self.file_name))
        if new_md5 == self.md5:
            return True
        else:
            os.remove(os.path.join(self.data_path, self.file_name))

    def get_md5(self, fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def unpack(self):
        Archive(os.path.join(self.data_path, self.file_name)).extractall(os.path.join(os.getcwd(), self.data_path))

    def split(self):
        self.unpack()
        file_to_split = os.path.join(self.data_path, os.path.splitext(self.file_name)[0])
        break_into = 'page'
        break_after = '1'
        parse(file_to_split, XMLBreaker(break_into, int(break_after), out=CycleFile(file_to_split)))
        os.remove(os.path.join(self.data_path, self.file_name))
        os.remove(file_to_split)
        return True

    def parse(self):
        results_base = os.path.join(self.data_path_base, 'results')
        if not os.path.isdir(results_base):
            os.makedirs(results_base)

        page_info_results_file = os.path.join(results_base, 'page_info.csv')
        revision_info_results_file = os.path.join(results_base, 'revisions.csv')
        no_text_error_results_file = os.path.join(results_base, 'no_text_error.csv')

        results_path = os.path.join(results_base, os.path.splitext(self.file_name)[0])
        if not os.path.isdir(results_path):
            os.makedirs(results_path)
        cat_results_file = os.path.join(results_path, 'cats.csv')
        link_results_file = os.path.join(results_path, 'links.csv')

        for file in glob.glob(self.data_path+'/*'):
            size = os.path.getsize(file)
            if size < 10485760000:
                for event, elem in etree.iterparse(file, tag='{http://www.mediawiki.org/xml/export-0.10/}page',
                                                   huge_tree=True):
                    for data in elem.iterchildren(reversed=False, tag='{http://www.mediawiki.org/xml/export-0.10/}ns'):
                        ns = data.text
                    if ns == '0' or ns == '14':
                        page_info, revision_info, no_text_error = self.get_data(elem, cat_results_file,
                                                                                link_results_file)
                        page_info.to_csv(page_info_results_file, sep='\t', mode='a', header=False, index=False)
                        revision_info.to_csv(revision_info_results_file, sep='\t', mode='a', header=False, index=False)
                        no_text_error.to_csv(no_text_error_results_file, sep='\t', mode='a', header=False, index=False)
                    else:
                        pass
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]


                # Remove parsed file
                os.remove(file)
            else:
                #print('TOOOO LARGE')
                #print(file)
                too_large = os.path.join(self.data_path_base, 'too_large_to_parse')
                if not os.path.isdir(too_large):
                    os.makedirs(too_large)
                #print(os.path.split(file[1]))
                try:
                    subprocess.call(['7z', 'a', os.path.join(os.getcwd(), file + '.7z'), os.path.join(os.getcwd(), file)], shell=True)
                    shutil.copy2(file+'.7z', too_large)
                    os.remove(file)
                    os.remove(file+'.7z')
                except:
                    pass

        # Handle results
        #subprocess.call(['7z', 'a', cat_results_file + '.7z', cat_results_file])
        #subprocess.call(['7z', 'a', link_results_file + '.7z', link_results_file])
        #os.remove(cat_results_file)
        #os.remove(link_results_file)
        return True

    def get_data(self, page, cat_results_file, link_results_file):
        # Besser als Pandas DF?!
        #page_info = np.empty([0, 4])
        page_info = pd.DataFrame(columns=['page_id', 'page_title', 'page_ns', 'date_created'])
        #revision_info = np.empty([0, 3])
        revision_info = pd.DataFrame(columns=['page_id', 'rev_id', 'rev_time'])
        #no_text_error = np.empty([0, 2])
        no_text_error = pd.DataFrame(columns=['page_id', 'rev_id'])
        page_title = 'NULL'
        page_id = 'NULL'
        page_ns = 'NULL'
        rev_id = 'NULL'
        rev_time = 'NULL'
        rev_text = 'NULL'
        rev_parent = 'NULL'
        rev_links = []
        rev_cats = []

        # Get data for page_info
        for elem in page.iterchildren(reversed=False, tag=None):
            if elem.tag == '{http://www.mediawiki.org/xml/export-0.10/}title':
                page_title = elem.text
            elif elem.tag == '{http://www.mediawiki.org/xml/export-0.10/}id':
                page_id = elem.text
            elif elem.tag == '{http://www.mediawiki.org/xml/export-0.10/}ns':
                page_ns = elem.text

        # Get data for revision_info
        for revision in page.iterchildren(reversed=False, tag='{http://www.mediawiki.org/xml/export-0.10/}revision'):
            include = False
            for elem in revision.iterchildren(reversed=False, tag=None):
                if elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}timestamp":
                    if parser.parse(elem.text).timestamp() >= self.start_date:
                        include = True
                elif elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}parentid":
                    rev_parent = elem.text
                elif elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}timestamp":
                    rev_time = elem.text
                else:
                    pass
            if include:
                for elem in revision.iterchildren(reversed=False, tag=None):
                    if elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}id":
                        rev_id = elem.text
                    elif elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}parentid":
                        rev_parent = elem.text
                    elif elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}timestamp":
                        rev_time = elem.text
                    elif elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}text":
                        rev_cats, rev_links = self.links(elem.text)
                        if not rev_links == 'ERROR':
                            for link in rev_links:
                                with open(link_results_file, 'a') as outfile:
                                    outfile.write(page_id + '\t' + rev_id + '\t' + link + '\n')
                        else:

                            no_text_error = no_text_error.append(pd.DataFrame([[page_id, rev_id]],
                                                                              columns=['page_id', 'rev_id']))
                        if not rev_cats == 'ERROR':
                            for cat in rev_cats:
                                with open(cat_results_file, 'a') as outfile:
                                    outfile.write(page_id + '\t' + rev_id + '\t' + cat + '\n')

                # Write data for revision_info
                revision_info = revision_info.append(pd.DataFrame([[page_id, rev_id, rev_time]], columns=['page_id', 'rev_id', 'rev_time']))

            # Write data for page_info, include time of the first revision for the creation time of the page
            if rev_parent == 'NULL':

                page_info = page_info.append(pd.DataFrame([[page_id, page_title, page_ns, rev_time]],
                                                          columns=['page_id', 'page_title', 'page_ns', 'date_created']))


        return page_info, revision_info, no_text_error


    # Returns two lists (cats and links) containing each only links to articles and links to categories
    def links(self, text):
        # Extract links from revision text.
        # Returns: links
        try:
            if re.search(r"\[\[Category\:(.*?)\]\]", text):
                cats = re.findall(r"\[\[Category\:(.*?)\]\]", text)
                cats = ['Category:'+x for x in cats]
            else:
                cats = []
        except:
            cats = 'ERROR'

        try:
            if re.search(r"(?!\[\[(?:[A-Za-z]+\:))\[\[(.*?)\]\]", text):
                links = re.findall(r"(?!\[\[(?:[A-Za-z]+\:))\[\[(.*?)\]\]", text)
            else:
                links = []
        except:
            links = 'ERROR'
        return cats, links

    def postprocessing_cat_link(self):
        #print('start postprocessing')
        results_base = os.path.join(self.data_path_base, 'results')
        relevant_revisions_file = os.path.join(results_base, 'relevant_revisions.csv')
        results_path = os.path.join(results_base, os.path.splitext(self.file_name)[0])
        cat_results_file = os.path.join(results_path, 'cats.csv')
        link_results_file = os.path.join(results_path, 'links.csv')
        #print('start postprocessing categories')
        cat_results_file = self.process_categories(cat_results_file)
        #print('start postprocessing links')
        link_results_file = self.process_links(link_results_file)
        #print('start postprocessing relevant revisions')
        relevant_revisions = self.assemble_list_of_relevant_revisions(cat_results_file, link_results_file)
        relevant_revisions.to_csv(relevant_revisions_file, sep='\t', index=False, header=False, mode='a')
        #print('COMPRESS CAT RESULTS')

        try:
            subprocess.call(['7z', 'a', os.path.join(os.getcwd(), cat_results_file + '.7z'),
                             os.path.join(os.getcwd(), cat_results_file)], shell=True)
            os.remove(cat_results_file)
        #print('COMPRESS LINK RESULTS')
        except:
            pass
        try:
            subprocess.call(['7z', 'a', os.path.join(os.getcwd(), link_results_file + '.7z'),
                             os.path.join(os.getcwd(), link_results_file)], shell=True)
            os.remove(link_results_file)
        except:
            pass

        return True

    def process_categories(self, cat_file):
        tmp_results_file = cat_file + 'tmp_results.csv'
        chunksize = 10 ** 6
        for chunk in pd.read_csv(cat_file, header=None, delimiter='\t', names=['page_id', 'rev_id', 'target'],
                                 chunksize=chunksize):
            chunk['target'] = chunk['target'].astype(str)
            chunk = chunk.dropna().reset_index().drop('index', 1)
            chunk = self.clean_labels(chunk, 'target')
            chunk = chunk.dropna().reset_index().drop('index', 1)
            chunk = self.unique_revisions(chunk)
            chunk.to_csv(tmp_results_file, sep='\t', index=False, header=False, mode='a')
        os.remove(cat_file)
        os.rename(tmp_results_file, cat_file)
        return cat_file

    def process_links(self, link_file):
        tmp_results_file = link_file+'tmp_results.csv'
        chunksize = 10 ** 6
        for chunk in pd.read_csv(link_file, header=None, delimiter='\t', names=['page_id', 'rev_id', 'target'],
                                 chunksize=chunksize):
            chunk['target'] = chunk['target'].astype(str)
            chunk = chunk.dropna().reset_index().drop('index', 1)
            chunk = self.clean_labels(chunk, 'target')
            chunk = chunk.dropna().reset_index().drop('index', 1)
            chunk = self.unique_revisions(chunk)
            chunk.to_csv(tmp_results_file, sep='\t', index=False, header=False, mode='a')

        os.remove(link_file)
        os.rename(tmp_results_file, link_file)
        return link_file

    def clean_labels(self, df, dimension):
        for row in df.itertuples():
            if bool(re.search(r"(.*?)[\#\|]", getattr(row, dimension))):
                clean_title = re.search(r"(.*?)[\#\|]", getattr(row, dimension)).group(1)
                df.set_value(row.Index, dimension, clean_title)
            else:
                pass
        df = df.drop_duplicates()
        return df

    def unique_revisions(self, df):
        curr_page = 0
        unique = pd.DataFrame()
        for name, group in df.groupby(['page_id', 'rev_id']):
            group_sorted = group.sort_values(['target'])
            if not name[0] == curr_page:
                curr_page = name[0]
                curr_labels = group_sorted.values
                unique = unique.append(group, ignore_index=True)
            elif not np.array_equiv(curr_labels.transpose()[2], group_sorted.values.transpose()[2]):
                unique = unique.append(group, ignore_index=True)
                curr_labels = group_sorted.values
            else:
                pass
        return unique

    def assemble_list_of_relevant_revisions(self, cat_file, link_file):
        results = pd.DataFrame()
        results = self.read_revisions(cat_file, results)
        results = self.read_revisions(link_file, results)
        results = results.drop_duplicates().reset_index().drop('index', 1)
        return results

    def read_revisions(self, file, results):
        chunksize = 10 ** 6
        for chunk in pd.read_csv(file, delimiter='\t', names=['page_id', 'rev_id', 'target'], chunksize=chunksize):
            tmp_data = pd.DataFrame()
            tmp_data = chunk['rev_id']
            tmp_data = tmp_data.to_frame()
            tmp_data = tmp_data.drop_duplicates()
            results = results.append(tmp_data)
        return results

    #def update_revisions_file(rev_file, cat_file, link_file):
    #    relevant_revs = assemble_revision_list(cat_file, link_file)
    #    rev_data = pd.read_csv(rev_file, delimiter='\t', names=['page_id', 'rev_id', 'ts'])
    #    rev_data = rev_data[rev_data['rev_id'].isin(relevant_revs['rev_id'])].reset_index().drop('index', 1)
    #    rev_data.to_csv(rev_file, sep='\t', index=False, header=False, mode='w')


class CycleFile(object):
    def __init__(self, filename):
        self.basename, self.ext = os.path.splitext(filename)
        self.index = 0
        self.open_next_file()

    def open_next_file(self):
        self.index += 1
        self.file = open(self.name(), 'w')

    def name(self):
        return '%s%s%s' % (self.basename, self.index, self.ext)

    def cycle(self):
        self.file.close()
        self.open_next_file()

    def write(self, str):
        try:
            str = str.decode('utf-8')
        except:
            pass
        self.file.write(str)

    def close(self):
        self.file.close()


# Splitting large XML files into smaller files, based on element is based on
# https://gist.github.com/nicwolff/b4da6ec84ba9c23c8e59
class XMLBreaker(XMLGenerator):
    def __init__(self, break_into=None, break_after=1000, out=None, *args, **kwargs):
        XMLGenerator.__init__(self, out, encoding='utf-8', *args, **kwargs)
        # XMLGenerator.__init__(self, out, *args, **kwargs)
        self.out_file = out
        self.break_into = break_into
        self.break_after = break_after
        self.context = []
        self.count = 0

    def startElement(self, name, attrs):
        XMLGenerator.startElement(self, name, attrs)
        self.context.append((name, attrs))

    def endElement(self, name):
        XMLGenerator.endElement(self, name)
        self.context.pop()
        if name == self.break_into:
            self.count += 1
            if self.count == self.break_after:
                self.count = 0
                for element in reversed(self.context):
                    self.out_file.write("\n")
                    XMLGenerator.endElement(self, element[0])
                self.out_file.cycle()
                XMLGenerator.startDocument(self)
                for element in self.context:
                    XMLGenerator.startElement(self, *element)


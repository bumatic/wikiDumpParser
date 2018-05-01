# =============================================================================
# Preprocessing adapted from wikiextractor Version: 2.75 (March 4, 2017)
# https://github.com/attardi/wikiextractor
#
#  By:
#  Author: Giuseppe Attardi (attardi@di.unipi.it), University of Pisa
#
#  Contributors:
#   Antonio Fuschetto (fuschett@aol.com)
#   Leonardo Souza (lsouza@amtera.com.br)
#   Juan Manuel Caicedo (juan@cavorite.com)
#   Humberto Pereira (begini@gmail.com)
#   Siegfried-A. Gevatter (siegfried@gevatter.com)
#   Pedro Assis (pedroh2306@gmail.com)
#   Wim Muskee (wimmuskee@gmail.com)
#   Radics Geza (radicsge@gmail.com)
#   orangain (orangain@gmail.com)
#   Seth Cleveland (scleveland@turnitin.com)
#   Bren Barn
#
# =============================================================================
#  Copyright (c) 2011-2017. Giuseppe Attardi (attardi@di.unipi.it).
# =============================================================================
#  This file is part of Tanl.
#
#  Tanl is free software; you can redistribute it and/or modify it
#  under the terms of the GNU General Public License, version 3,
#  as published by the Free Software Foundation.
#
#  Tanl is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License at <http://www.gnu.org/licenses/> for more details.
#
# =============================================================================


from __future__ import unicode_literals, division
import os
from pyunpack import Archive
import requests
from retrying import retry
from xml.sax import parse
from xml.sax.saxutils import XMLGenerator
import numpy as np
from lxml import etree
import shutil
import glob
import subprocess
from dateutil import parser
import pandas as pd
import hashlib
import random
import time
import codecs
import fileinput
# import logging
import re  # TODO use regex when it will be standard
from timeit import default_timer
from html.entities import name2codepoint
from types import SimpleNamespace


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

        self.options = SimpleNamespace(
            ##
            # The namespace used for template definitions
            # It is the name associated with namespace key=10 in the siteinfo header.
            templateNamespace='',
            templatePrefix='',

            ##
            # The namespace used for module definitions
            # It is the name associated with namespace key=828 in the siteinfo header.
            moduleNamespace='',
            modulePrefix='',

            ##
            # Shared objects holding templates, redirects and cache
            templates={},
            redirects={},
        )
        self.text_type = str

        ##
        # Keys for Template and Module namespaces
        self.templateKeys = set(['10', '828'])

        ##
        # Regex for identifying disambig pages
        self.filter_disambig_page_pattern = re.compile("{{disambig(uation)?(\|[^}]*)?}}")

        # Match HTML comments
        # The buggy template {{Template:T}} has a comment terminating with just "->"
        self.comment = re.compile(r'<!--.*?-->', re.DOTALL)

        # Match <nowiki>...</nowiki>
        self.nowiki = re.compile(r'<nowiki>.*?</nowiki>')

        # Extract Template definition
        self.reNoinclude = re.compile(r'<noinclude>(?:.*?)</noinclude>', re.DOTALL)
        self.reIncludeonly = re.compile(r'<includeonly>|</includeonly>', re.DOTALL)

        self.tagRE = re.compile(r'(.*?)<(/?\w+)[^>]*?>(?:([^<]*)(<.*?>)?)?')
        #                           1       2              3       4
        self.keyRE = re.compile(r'key="(\d*)"')

    def process(self):
        if self.status == 'preprocessed':
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
    def unpack(self):
        Archive(os.path.join(self.data_path, self.file_name)).extractall(os.path.join(os.getcwd(), self.data_path))

    def parse(self):
        results_base = os.path.join(self.data_path_base, 'results')
        if not os.path.isdir(results_base):
            os.makedirs(results_base)

        page_info_results_file = os.path.join(results_base, 'page_info.csv')
        revision_info_results_file = os.path.join(results_base, 'revisions.csv')
        no_text_error_results_file = os.path.join(results_base, 'no_text_error.csv')
        author_info_results_file = os.path.join(results_base, 'author_info.csv')

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
                        page_info, revision_info, no_text_error, author_info = self.get_data(elem, cat_results_file, link_results_file)
                        page_info.to_csv(page_info_results_file, sep='\t', mode='a', header=False, index=False)
                        revision_info.to_csv(revision_info_results_file, sep='\t', mode='a', header=False, index=False)
                        no_text_error.to_csv(no_text_error_results_file, sep='\t', mode='a', header=False, index=False)
                        author_info.to_csv(author_info_results_file, sep='\t', mode='a', header=False, index=False)
                    else:
                        pass
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]
                os.remove(file)
            else:
                too_large = os.path.join(self.data_path_base, 'too_large_to_parse')
                if not os.path.isdir(too_large):
                    os.makedirs(too_large)
                try:
                    subprocess.call(['7z', 'a', os.path.join(os.getcwd(), file + '.7z'),
                                     os.path.join(os.getcwd(), file)])
                    shutil.copy2(file+'.7z', too_large)
                    os.remove(file)
                    os.remove(file+'.7z')
                except:
                    pass
        return True

    def get_data(self, page, cat_results_file, link_results_file):
        page_info = pd.DataFrame(columns=['page_id', 'page_title', 'page_ns', 'date_created'])
        revision_info = pd.DataFrame(columns=['page_id', 'rev_id', 'rev_time', 'rev_author_id'])
        author_info = pd.DataFrame(columns=['rev_author_id', 'rev_author_name'])
        no_text_error = pd.DataFrame(columns=['page_id', 'rev_id'])
        page_title = 'NULL'
        page_id = 'NULL'
        page_ns = 'NULL'
        rev_id = 'NULL'
        rev_time = 'NULL'
        rev_author_name = 'NULL'
        rev_author_id = 'NULL'
        rev_parent = 'NULL'

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
                elif elem.tag == "{http://www.mediawiki.org/xml/export-0.10/}contributor":
                    for item in elem.iterchildren(reversed=False, tag=None):
                        if item.tag == "{http://www.mediawiki.org/xml/export-0.10/}username":
                            rev_author_name = item.text
                        elif item.tag == "{http://www.mediawiki.org/xml/export-0.10/}id":
                            rev_author_id = item.text
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
                revision_info = revision_info.append(pd.DataFrame([[page_id, rev_id, rev_time, rev_author_id]],
                                                                  columns=['page_id', 'rev_id', 'rev_time', 'rev_author_id']))
                # Write data for author_info
                author_info = author_info.append(pd.DataFrame([[rev_author_id, rev_author_name]],
                                                              columns=['rev_author_id', 'rev_author_name']))

            # Write data for page_info, include time of the first revision for the creation time of the page
            if rev_parent == 'NULL':
                page_info = page_info.append(pd.DataFrame([[page_id, page_title, page_ns, rev_time]],
                                                          columns=['page_id', 'page_title', 'page_ns', 'date_created']))
        return page_info, revision_info, no_text_error, author_info

    # Returns two lists (cats and links) containing each only links to articles and links to categories
    @staticmethod
    def links(text):
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
        results_base = os.path.join(self.data_path_base, 'results')
        relevant_revisions_file = os.path.join(results_base, 'relevant_revisions.csv')
        results_path = os.path.join(results_base, os.path.splitext(self.file_name)[0])
        cat_results_file = os.path.join(results_path, 'cats.csv')
        link_results_file = os.path.join(results_path, 'links.csv')
        try:
            cat_results_file = self.process_categories(cat_results_file)
        except:
            pass
        try:
            link_results_file = self.process_links(link_results_file)
        except:
            pass
        try:
            relevant_revisions = self.assemble_list_of_relevant_revisions(cat_results_file, link_results_file)
            relevant_revisions.to_csv(relevant_revisions_file, sep='\t', index=False, header=False, mode='a')
        except:
            pass
        try:
            subprocess.call(['7z', 'a', os.path.join(os.getcwd(), cat_results_file + '.7z'),
                             os.path.join(os.getcwd(), cat_results_file)])
            os.remove(cat_results_file)
        except:
            pass
        try:
            subprocess.call(['7z', 'a', os.path.join(os.getcwd(), link_results_file + '.7z'),
                             os.path.join(os.getcwd(), link_results_file)])
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

    @staticmethod
    def clean_labels(df, dimension):
        for row in df.itertuples():
            if bool(re.search(r"(.*?)[\#\|]", getattr(row, dimension))):
                clean_title = re.search(r"(.*?)[\#\|]", getattr(row, dimension)).group(1)
                df.set_value(row.Index, dimension, clean_title)
            else:
                pass
        df = df.drop_duplicates()
        return df

    @staticmethod
    def unique_revisions(df):
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

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

        # List of files larger than 10GB.
        # As of 1. March 2018 all of them are outside the scope of the parser and can be ignored.
        # Automatic handling needs to be implemented.
        self.ignore = [
            # TITLE: Wikipedia:Village pump (policy), NS: 4
            986140,
            # TITLE: Wikipedia:Administrators' noticeboard/Incidents, NS: 4
            5137507,
            # TITLE: Wikipedia:Reference desk/Miscellaneous, NS: 4
            40297,
            # TITLE: Template talk:Did you know, NS: 11
            972034,
            # TITLE: Wikipedia:Reference desk/Humanities , NS: 4
            2535875,
            # TITLE: Wikipedia:Reference desk/Science , NS: 4
            2535910,
            # TITLE: Wikipedia talk:Manual of Style , NS: 5
            75321,
            # TITLE: Wikipedia:Help desk , NS: 4
            564696,
            # TITLE: Wikipedia:Reference desk/Language , NS: 4
            2515121,
            # TITLE: Wikipedia talk:Requests for adminship , NS: 5
            2609426,
            # TITLE: Wikipedia:Village pump (technical) , NS: 4
            3252662,
            # TITLE: Wikipedia:Administrators' noticeboard , NS: 4
            5149102,
            # TITLE: Wikipedia:In the news/Candidates , NS: 4
            1470141,
            # TITLE: Wikipedia:Administrators' noticeboard/Edit warring , NS: 4
            3741656,
            # TITLE: Wikipedia:Good article nominations , NS: 4
            3514978,
            # TITLE: Wikipedia:Village pump (proposals) , NS: 4
            3706897,
            # TITLE: Wikipedia:Reference desk/Computing , NS: 4
            6041086,
            # TITLE: User talk:DGG , NS: 3
            6905700,
            # TITLE: Wikipedia:Requested moves/Current discussions (alt) , NS: 4
            23259666,
            # TITLE: Wikipedia:Biographies of living persons/Noticeboard , NS: 4
            6768170,
            # TITLE: User:COIBot/LinkReports , NS: 2
            10701605,
            # TITLE: Wikipedia:Reliable sources/Noticeboard , NS: 4
            11424955,
            # TITLE: User talk:Jimbo Wales , NS: 3
            9870625,
            # TITLE: Wikipedia:Arbitration/Requests/Enforcement , NS: 4
            12936136,
            # TITLE: User talk:ImageTaggingBot/log , NS: 3
            17820752,
            # TITLE: Wikipedia:WikiProject Spam/LinkReports , NS: 4
            16927404,
            # TITLE: User:JamesR/AdminStats , NS: 2
            18530389,
            # TITLE: Template:AFC statistics , NS: 10
            23309859,
            # TITLE: Wikipedia:Requested moves/Current discussions , NS: 4
            22998103,
            # TITLE: User:West.andrew.g/Dead links , NS: 2
            32101143,
            # TITLE: User:B-bot/Event log , NS: 2
            46505226,
            # TITLE: User:Pentjuuu!.!/sandbox , NS: 2
            42765277,
            # TITLE: Wikipedia:Teahouse , NS: 4
            34745517
        ]
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

    def preprocess(self):
        quiet = False
        debug = False
        self.createLogger(quiet, debug)

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
            success = self.get_templates()
            if success:
                new_status = 'preprocessed'
                return new_status

    def get_templates(self):
        #def get_templates(input_file, template_file):
        """
        :param input_file: wikipedia dump file
        :param template_file: optional file with template definitions.
        """
        print(os.path.join(self.data_path, self.file_name[:-3]))
        self.unpack()
        template_load_start = default_timer()
        logging.info("Preprocessing '%s' to collect template definitions: this may take some time.", self.file_name)
        template_file = os.path.join(self.data_path_base, 'templates', self.file_name[:-3])
        self.load_templates(os.path.join(self.data_path, self.file_name[:-3]), template_file)
        template_load_elapsed = default_timer() - template_load_start
        logging.info("Loaded %d templates in %.1fs", len(self.options.templates), template_load_elapsed)
        os.remove(os.path.join(self.data_path, self.file_name[:-3]))
        return True

    @staticmethod
    def unescape(text):
        """
        Removes HTML or XML character references and entities from a text string.

        :param text The HTML (or XML) source text.
        :return The plain text, as a Unicode string, if necessary.
        """

        def fixup(m):
            text = m.group(0)
            code = m.group(1)
            try:
                if text[1] == "#":  # character reference
                    if text[2] == "x":
                        return chr(int(code[1:], 16))
                    else:
                        return chr(int(code))
                else:  # named entity
                    return chr(name2codepoint[code])
            except:
                return text  # leave as is

        return re.sub("&#?(\w+);", fixup, text)

    def define_template(self, title, page):
        """
        Adds a template defined in the :param page:.
        @see https://en.wikipedia.org/wiki/Help:Template#Noinclude.2C_includeonly.2C_and_onlyinclude
        """
        # title = normalizeTitle(title)

        # sanity check (empty template, e.g. Template:Crude Oil Prices))
        if not page:
            return

        # check for redirects
        m = re.match('#REDIRECT.*?\[\[([^\]]*)]]', page[0], re.IGNORECASE)
        if m:
            self.options.redirects[title] = m.group(1)  # normalizeTitle(m.group(1))
            return

        text = self.unescape(''.join(page))
        # We're storing template text for future inclusion, therefore,
        # remove all <noinclude> text and keep all <includeonly> text
        # (but eliminate <includeonly> tags per se).
        # However, if <onlyinclude> ... </onlyinclude> parts are present,
        # then only keep them and discard the rest of the template body.
        # This is because using <onlyinclude> on a text fragment is
        # equivalent to enclosing it in <includeonly> tags **AND**
        # enclosing all the rest of the template body in <noinclude> tags.

        # remove comments
        text = self.comment.sub('', text)

        # eliminate <noinclude> fragments
        text = self.reNoinclude.sub('', text)
        # eliminate unterminated <noinclude> elements
        text = re.sub(r'<noinclude\s*>.*$', '', text, flags=re.DOTALL)
        text = re.sub(r'<noinclude/>', '', text)

        onlyincludeAccumulator = ''
        for m in re.finditer('<onlyinclude>(.*?)</onlyinclude>', text, re.DOTALL):
            onlyincludeAccumulator += m.group(1)
        if onlyincludeAccumulator:
            text = onlyincludeAccumulator
        else:
            text = self.reIncludeonly.sub('', text)

        if text:
            if title in self.options.templates:
                logging.warning('Redefining: %s', title)
            self.options.templates[title] = text

    def pages_from(self, input):
        """
        Scans input extracting pages.
        :return: (id, revid, title, namespace key, page), page is a list of lines.
        """
        # we collect individual lines, since str.join() is significantly faster
        # than concatenation

        page = []
        id = None
        ns = '0'
        last_id = None
        revid = None
        inText = False
        redirect = False
        title = None
        logging.debug('Parsing input %s', input)
        last_tag = None
        for line in input:
            if not isinstance(line, self.text_type): line = line.decode('utf-8')
            if '<' not in line:  # faster than doing re.search()
                if inText:
                    page.append(line)
                continue
            m = self.tagRE.search(line)
            if not m:
                continue
            tag = m.group(2)
            if tag == 'page':
                page = []
                redirect = False
            elif tag == 'id' and not id:
                id = m.group(3)
            elif tag == 'id' and id and last_tag != 'username':
                revid = m.group(3)
            elif tag == 'title':
                title = m.group(3)
            elif tag == 'ns':
                ns = m.group(3)
            elif tag == 'redirect':
                redirect = True
            elif tag == 'revision':
                page.append('<revision>\n')
            elif tag == '/revision':
                page.append('</revision>\n')
            elif tag == 'timestamp':
                timestamp = m.group(3)
                page.append('<timestamp>%s</timestamp>\n' % timestamp)  # <ns>%s</ns>\n' % ns
                # page.append(timestamp)
                # page.append('</timestamp>\n')
            elif tag == 'username':
                username = m.group(3)
            elif tag == 'id' and last_tag == 'username':
                uid = m.group(3)
                page.append('<contributor>\n')
                page.append('<username>%s</username>\n' % username)
                # page.append(username)
                # page.append('</username>')
                page.append('<id>%s</id>\n' % uid)
                # page.append(uid)
                # page.append('</id>')
                page.append('</contributor>\n')
                del username
                del uid
            elif tag == 'text':
                if m.lastindex == 3 and line[m.start(3) - 2] == '/':  # self closing
                    # <text xml:space="preserve" />
                    continue
                else:
                    page.append('<text xml:space="preserve" />\n')
                inText = True
                line = line[m.start(3):m.end(3)]
                page.append(line)
                if m.lastindex == 4:  # open-close
                    inText = False
            elif tag == '/text':
                if m.group(1):
                    page.append(m.group(1))
                page.append('</text>\n')
                inText = False
            elif inText:
                page.append(line)
            elif tag == '/page':
                if id != last_id and not redirect:
                    yield (id, revid, title, ns, page)
                    last_id = id
                    ns = '0'
                id = None
                revid = None
                title = None
                page = []
            last_tag = tag

    def load_templates(self, input_file, output_file=None):
        """
        Load templates from :param file:.
        :param output_file: file where to save templates and modules.
        """
        # pages_from()
        # page_data??
        # define_template()

        input = fileinput.FileInput(input_file, openhook=fileinput.hook_compressed)

        self.options.templatePrefix = self.options.templateNamespace + ':'
        self.options.modulePrefix = self.options.moduleNamespace + ':'

        if output_file:
            logging.debug('Output file %s for template in %s created', output_file, input_file)
            output = codecs.open(output_file, 'wb', 'utf-8')
        for page_count, page_data in enumerate(self.pages_from(input)):
            id, revid, title, ns, page = page_data
            if ns in self.templateKeys:
                text = ''.join(page)
                self.define_template(title, text)
                # save templates and modules to file
                if output_file:
                    output.write('<page>\n')
                    output.write('   <title>%s</title>\n' % title)
                    output.write('   <ns>%s</ns>\n' % ns)
                    output.write('   <id>%s</id>\n' % id)
                    for line in page:
                        output.write(line)
                    output.write('</page>\n')
            if page_count and page_count % 1000 == 0:
            #if page_count and page_count % 100000 == 0:
                logging.info("Preprocessed %d pages", page_count)
        if output_file:
            output.close()
            logging.info("Saved %d templates to '%s'", len(self.options.templates), output_file)

    @staticmethod
    def createLogger(quiet, debug):
        logger = logging.getLogger()
        if not quiet:
            logger.setLevel(logging.INFO)
        if debug:
            logger.setLevel(logging.DEBUG)

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

    @staticmethod
    def get_md5(fname):
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

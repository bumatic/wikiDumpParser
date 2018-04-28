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
import hashlib
import random
import time
import codecs
import fileinput
import logging
import re  # TODO use regex when it will be standard
from timeit import default_timer
from html.entities import name2codepoint
from types import SimpleNamespace
import dill


class PreProcessor:
    def __init__(self, file_name, data_path, base_url, status, start_date, md5):  #, logger
        self.file_name = file_name
        self.data_path_base = data_path
        self.data_path = os.path.join(self.data_path_base, os.path.splitext(self.file_name)[0])
        if not os.path.isdir(os.path.join(os.getcwd(), self.data_path)):
            os.makedirs(os.path.join(os.getcwd(), self.data_path))
        self.base_url = base_url
        self.status = status
        self.start_date = start_date
        self.md5 = md5
        #self.logger = logging.getLogger()
        #self.logger.setLevel(logging.INFO)
        #self.logger = logger

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
            # 23309859,
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
        #quiet = False
        #debug = False
        #self.createLogger(quiet, debug)
        logging.info('Start preprocessing %s.' % self.file_name)
        if self.status == 'init':
            success = self.download_dump_file()
            if success:
                new_status = 'downloaded'
                return new_status
            else:
                new_status = 'init'
                logging.info("Problem downloading '%s'. Retrying in 60 to 120 seconds." % self.file_name)
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

        self.unpack()
        template_load_start = default_timer()
        logging.info("Preprocessing '%s' to collect template definitions: this may take some time." % self.file_name)
        template_file = os.path.join(self.data_path_base, 'templates', self.file_name[:-3])
        self.load_templates(os.path.join(self.data_path, self.file_name[:-3]), template_file)
        template_load_elapsed = default_timer() - template_load_start
        logging.info("Loaded %d templates in %.1fs" % len(self.options.templates) % template_load_elapsed)
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
                logging.warning('Redefining: %s' % title)
                pass

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
        logging.debug('Parsing input %s' % input)
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

    def load_templates(self, input_file, output_file):
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

        output = codecs.open(output_file, 'wb', 'utf-8')
        logging.debug('Output file %s for template in %s created' % output_file % input_file)
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
            #if page_count and page_count % 1000 == 0:
            if page_count and page_count % 100000 == 0:
                logging.info("File %s:Preprocessed %d pages" % self.file_name % page_count)
                pass
        output.close()
        logging.info("Saved %d templates to '%s'" % len(self.options.templates) % output_file)

    @retry(wait_random_min=1000, wait_random_max=20000, stop_max_attempt_number=20)
    def download_dump_file(self):
        logging.info('Start downloading file %s' % self.file_name)
        x = random.randint(1, 120)
        time.sleep(x)
        response = requests.get(self.base_url + self.file_name, stream=True)
        with open(os.path.join(self.data_path, self.file_name), "wb") as handle:
            for data in response.iter_content(chunk_size=32768):
                handle.write(data)
        new_md5 = self.get_md5(os.path.join(self.data_path, self.file_name))
        if new_md5 == self.md5:
            logging.info("Successfully downloaded '%s'." % self.file_name)
            return True
        else:
            os.remove(os.path.join(self.data_path, self.file_name))
            logging.info("Downloading '%s' failed. Retry later." % self.file_name)
            return False

    @staticmethod
    def get_md5(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def unpack(self):
        Archive(os.path.join(self.data_path, self.file_name)).extractall(os.path.join(os.getcwd(), self.data_path))
        logging.info("Unpacked file '%s'." % self.file_name)


#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
#  Version: 2.75 (March 4, 2017)
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

import sys
import argparse
import bz2
import codecs
import cgi
import fileinput
#import logging
import os.path
import re  # TODO use regex when it will be standard
import time
import json
from io import StringIO
from multiprocessing import Queue, Process, Value, cpu_count
from timeit import default_timer
from urllib.parse import quote
from html.entities import name2codepoint
from itertools import zip_longest
from types import SimpleNamespace

text_type = str

## PARAMS ####################################################################

options = SimpleNamespace(

    ##
    # Defined in <siteinfo>
    # We include as default Template, when loading external template file.
    knownNamespaces={'Template': 10},

    ##
    # The namespace used for template definitions
    # It is the name associated with namespace key=10 in the siteinfo header.
    templateNamespace='',
    templatePrefix='',

    ##
    # The namespace used for module definitions
    # It is the name associated with namespace key=828 in the siteinfo header.
    moduleNamespace='',

    ##
    # Recognize only these namespaces in links
    # w: Internal links to the Wikipedia
    # wiktionary: Wiki dictionary
    # wikt: shortcut for Wiktionary
    #
    acceptedNamespaces=['w', 'wiktionary', 'wikt'],

    # This is obtained from <siteinfo>
    urlbase='',

    ##
    # Filter disambiguation pages
    filter_disambig_pages=False,

    ##
    # Drop tables from the article
    keep_tables=False,

    ##
    # Whether to preserve links in output
    keepLinks=False,

    ##
    # Whether to preserve section titles
    keepSections=True,

    ##
    # Whether to preserve lists
    keepLists=False,

    ##
    # Whether to output HTML instead of text
    toHTML=False,

    ##
    # Whether to write json instead of the xml-like default output format
    write_json=False,

    ##
    # Whether to expand templates
    expand_templates=True,

    ##
    ## Whether to escape doc content
    escape_doc=False,

    ##
    # Print the wikipedia article revision
    print_revision=False,

    ##
    # Minimum expanded text length required to print document
    min_text_length=0,

    # Shared objects holding templates, redirects and cache
    templates={},
    redirects={},
    # cache of parser templates
    # FIXME: sharing this with a Manager slows down.
    templateCache={},

    # Elements to ignore/discard

    ignored_tag_patterns=[],

    discardElements=[
        'gallery', 'timeline', 'noinclude', 'pre',
        'table', 'tr', 'td', 'th', 'caption', 'div',
        'form', 'input', 'select', 'option', 'textarea',
        'ul', 'li', 'ol', 'dl', 'dt', 'dd', 'menu', 'dir',
        'ref', 'references', 'img', 'imagemap', 'source', 'small',
        'sub', 'sup', 'indicator'
    ],
)

##
# Keys for Template and Module namespaces
templateKeys = set(['10', '828'])

##
# Regex for identifying disambig pages
filter_disambig_page_pattern = re.compile("{{disambig(uation)?(\|[^}]*)?}}")

# Match HTML comments
# The buggy template {{Template:T}} has a comment terminating with just "->"
comment = re.compile(r'<!--.*?-->', re.DOTALL)

# Match <nowiki>...</nowiki>
nowiki = re.compile(r'<nowiki>.*?</nowiki>')


# ======================================================================

#substWords = 'subst:|safesubst:'

#magicWordsRE = re.compile('|'.join(MagicWords.switches))


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


# Extract Template definition

reNoinclude = re.compile(r'<noinclude>(?:.*?)</noinclude>', re.DOTALL)
reIncludeonly = re.compile(r'<includeonly>|</includeonly>', re.DOTALL)


def define_template(title, page):
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
        options.redirects[title] = m.group(1)  # normalizeTitle(m.group(1))
        return

    text = unescape(''.join(page))
    # We're storing template text for future inclusion, therefore,
    # remove all <noinclude> text and keep all <includeonly> text
    # (but eliminate <includeonly> tags per se).
    # However, if <onlyinclude> ... </onlyinclude> parts are present,
    # then only keep them and discard the rest of the template body.
    # This is because using <onlyinclude> on a text fragment is
    # equivalent to enclosing it in <includeonly> tags **AND**
    # enclosing all the rest of the template body in <noinclude> tags.

    # remove comments
    text = comment.sub('', text)

    # eliminate <noinclude> fragments
    text = reNoinclude.sub('', text)
    # eliminate unterminated <noinclude> elements
    text = re.sub(r'<noinclude\s*>.*$', '', text, flags=re.DOTALL)
    text = re.sub(r'<noinclude/>', '', text)

    onlyincludeAccumulator = ''
    for m in re.finditer('<onlyinclude>(.*?)</onlyinclude>', text, re.DOTALL):
        onlyincludeAccumulator += m.group(1)
    if onlyincludeAccumulator:
        text = onlyincludeAccumulator
    else:
        text = reIncludeonly.sub('', text)

    if text:
        if title in options.templates:
            logging.warning('Redefining: %s', title)
        options.templates[title] = text

def pages_from(input):
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
        if not isinstance(line, text_type): line = line.decode('utf-8')
        if '<' not in line:  # faster than doing re.search()
            if inText:
                page.append(line)
            continue
        m = tagRE.search(line)
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
            page.append('<timestamp>%s</timestamp>\n' % timestamp) # <ns>%s</ns>\n' % ns
            #page.append(timestamp)
            #page.append('</timestamp>\n')
        elif tag == 'username':
            username = m.group(3)
        elif tag == 'id' and last_tag == 'username':
            uid = m.group(3)
            page.append('<contributor>\n')
            page.append('<username>%s</username>\n' % username)
            #page.append(username)
            #page.append('</username>')
            page.append('<id>%s</id>\n' % uid)
            #page.append(uid)
            #page.append('</id>')
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



tagRE = re.compile(r'(.*?)<(/?\w+)[^>]*?>(?:([^<]*)(<.*?>)?)?')
#                      1     2               3      4
keyRE = re.compile(r'key="(\d*)"')


def load_templates(file, output_file=None):
    """
    Load templates from :param file:.
    :param output_file: file where to save templates and modules.
    """
    # pages_from()
    # page_data??
    # define_template()

    input = fileinput.FileInput(input_file, openhook=fileinput.hook_compressed)

    options.templatePrefix = options.templateNamespace + ':'
    options.modulePrefix = options.moduleNamespace + ':'

    if output_file:
        logging.debug('Output file %s for template in %s created', output_file, file)
        output = codecs.open(output_file, 'wb', 'utf-8')
    for page_count, page_data in enumerate(pages_from(input)):
        id, revid, title, ns, page = page_data
        if ns in templateKeys:
            text = ''.join(page)
            define_template(title, text)
            # save templates and modules to file
            if output_file:
                output.write('<page>\n')
                output.write('   <title>%s</title>\n' % title)
                output.write('   <ns>%s</ns>\n' % ns)
                output.write('   <id>%s</id>\n' % id)
                #output.write('   <text>')
                for line in page:
                    output.write(line)
                #output.write('   </text>\n')
                output.write('</page>\n')
        if page_count and page_count % 100000 == 0:
            logging.info("Preprocessed %d pages", page_count)
    if output_file:
        output.close()
        logging.info("Saved %d templates to '%s'", len(options.templates), output_file)


def get_templates(input_file, template_file):
    """
    :param input_file: wikipedia dump file
    :param template_file: optional file with template definitions.
    """

    template_load_start = default_timer()
    logging.info("Preprocessing '%s' to collect template definitions: this may take some time.", input_file)
    load_templates(input_file, template_file)
    template_load_elapsed = default_timer() - template_load_start
    logging.info("Loaded %d templates in %.1fs", len(options.templates), template_load_elapsed)


# ----------------------------------------------------------------------

def createLogger(quiet, debug):
    logger = logging.getLogger()
    if not quiet:
        logger.setLevel(logging.INFO)
    if debug:
        logger.setLevel(logging.DEBUG)

# ----------------------------------------------------------------------
'''
options.keepLinks = False
options.keepSections = False
options.keepLists = False
options.toHTML = False
options.write_json = False
options.print_revision = False
options.min_text_length = False
options.expand_templates = True
options.filter_disambig_pages = False
options.keep_tables = False
file_size = 200 * 1024
'''
options.quiet = False
options.debug = True

createLogger(options.quiet, options.debug)

#output_path = args.output

#input_file = 'TEMPLATES_enwiki-20161101-pages-meta-history1.xml-p000000010p000002289_out_100Thousand_lines.xml'
input_file = 'enwiki-20180401-pages-meta-history12.xml-p4111770p4170970'
get_templates(input_file, 'TEMPLATE.xml')


import os
from xml.sax import parse
from xml.sax.saxutils import XMLGenerator
from pyunpack import Archive
import requests
from retrying import retry
import hashlib
import random
import time
import fileinput
import re
import glob
import shutil
import subprocess
# from timeit import default_timer
# from html.entities import name2codepoint
# from types import SimpleNamespace


class PreProcessor:
    def __init__(self, file_name, data_path, base_url, status, start_date, md5, debug):
        self.file_name = file_name
        self.data_path_base = data_path
        self.data_path = os.path.join(self.data_path_base, os.path.splitext(self.file_name)[0])
        if not os.path.isdir(os.path.join(os.getcwd(), self.data_path)):
            os.makedirs(os.path.join(os.getcwd(), self.data_path))
        self.templates_path = os.path.join(self.data_path_base, 'templates')
        if not os.path.isdir(os.path.join(os.getcwd(), self.templates_path)):
            os.makedirs(os.path.join(os.getcwd(), self.templates_path))
        self.base_url = base_url
        self.status = status
        self.start_date = start_date
        self.md5 = md5
        self.debug = debug
        self.templateKeys = set(['10', '828'])
        self.pageKeys = set(['1', '14'])

        self.text_type = str
        self.tagRE = re.compile(r'(.*?)<(/?\w+)[^>]*?>(?:([^<]*)(<.*?>)?)?')
        #                           1       2              3       4

        '''
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

        
        self.keyRE = re.compile(r'key="(\d*)"')
        '''

    def process(self):
        if self.debug:
            print('Start pre-processing {0}.'.format(self.file_name))
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
            success = self.split()
            if success:
                new_status = 'split'
                return new_status
            else:
                new_status = 'error'
                return new_status
        if self.status == 'split':
            success = self.filter_relevant()
            if success:
                new_status = 'preprocessed'
                return new_status
            else:
                new_status = 'error'
                return new_status

    def get_info(self, finput):
        pid = None
        ns = None
        for line in finput:
            if not isinstance(line, self.text_type):
                line = line.decode('utf-8')
            m = self.tagRE.search(line)
            if not m:
                continue
            tag = m.group(2)
            if tag == 'id' and not pid:
                pid = m.group(3)
            elif tag == 'ns':
                m.group(3)
            if pid is not None and ns is not None:
                return pid, ns

    def filter_relevant(self):
        for file in glob.glob(self.data_path+'/*'):
            finput = fileinput.FileInput(file, openhook=fileinput.hook_compressed)
            pid, ns = self.get_info(finput)
            if ns in self.templateKeys or ns in self.pageKeys:
                if ns in self.templateKeys:
                    new_file = os.path.join(os.path.split(file[0]), pid)
                    os.rename(file, new_file)
                    shutil.move(new_file, self.templates_path)
                elif ns in self.pageKeys:
                    new_file = os.path.join(os.path.split(file[0]), pid)
                    os.rename(file, new_file)
            else:
                os.remove(file)
        subprocess.call(['7z', 'a', self.data_path+'.7z', self.data_path])
        shutil.rmtree(self.data_path)
        return True

    @retry(wait_random_min=1000, wait_random_max=20000, stop_max_attempt_number=20)
    def download_dump_file(self):
        if self.debug:
            print('Start downloading file {0}'.format(self.file_name))
        x = random.randint(1, 120)
        time.sleep(x)
        response = requests.get(self.base_url + self.file_name, stream=True)
        with open(os.path.join(self.data_path, self.file_name), "wb") as handle:
            for data in response.iter_content(chunk_size=32768):
                handle.write(data)
        new_md5 = self.get_md5(os.path.join(self.data_path, self.file_name))
        if new_md5 == self.md5:
            #logging.info("Successfully downloaded '{0}'.".format(self.file_name))
            return True
        else:
            os.remove(os.path.join(self.data_path, self.file_name))
            #logging.info("Downloading '{0}' failed. Retry later.".format(self.file_name))
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
        if self.debug:
            print(("Unpacked file '{0}'.".format(self.file_name)))
        return

    def split(self):
        self.unpack()
        file_to_split = os.path.join(self.data_path, os.path.splitext(self.file_name)[0])
        break_into = 'page'
        break_after = '1'
        parse(file_to_split, XMLBreaker(break_into, int(break_after), out=CycleFile(file_to_split)))
        os.remove(os.path.join(self.data_path, self.file_name))
        os.remove(file_to_split)
        return True


# Splitting large XML files into smaller files, based on element is based on
# https://gist.github.com/nicwolff/b4da6ec84ba9c23c8e59
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

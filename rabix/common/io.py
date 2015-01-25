import tempfile
import logging
import json
import requests
import glob
import six

from copy import copy
from six.moves.urllib.parse import urlparse, urlunparse
from six.moves import input as raw_input
from os import mkdir
from os.path import basename, dirname, abspath, join, exists, isfile

from rabix.common.errors import ResourceUnavailable
from rabix.common.util import sec_files_naming_conv, to_json, to_abspath
from rabix.common.ref_resolver import from_url
from rabix.common.models import File, URL

log = logging.getLogger(__name__)


class InputCollector(object):
    """
    Will handle local files, 'data:,' URLs (for tests) and delegate other
    URLs to requests.get()
    """
    def __init__(self, job_dir):
        self.downloader = self.detect_downloader()
        self.dir = job_dir
        if not exists(job_dir):
            mkdir(job_dir)

    @staticmethod
    def detect_downloader():
        pass

    def download(self, url, secondary_files=None, prompt=True):
        npath = to_abspath(self._download(url, metasearch=True))
        rbx_path = npath + '.rbx.json'
        if isfile(rbx_path):
            file_dict = from_url(rbx_path)
            startdir = dirname(npath)
            file_dict['path'] = npath
            file = File(file_dict)
            file.secondary_files = [
                File(
                    self._download(
                        URL(to_abspath(sf.path, startdir)),
                        metasearch=False))
                for sf in file.secondary_files
            ]
        else:
            file = File(npath)
            file.meta = self._meta(npath, prompt=prompt)
            if secondary_files:
                file.secondary_files = self._get_secondary_files(
                    secondary_files, url, prompt=prompt)

            if prompt:
                self._rbx_dump(file)
        return file

    def _download(self, url, metasearch=True):
        if url.isdata():
            dest = tempfile.mktemp(dir=self.dir)
            with open(dest, 'w') as fp:
                fp.write(url.data)
            return dest

        if url.islocal():
            return url.path

        log.debug('Downloading %s', url)
        r = requests.get(url.geturl())
        try:
            r.raise_for_status()
        except Exception as e:
            log.error('Resource unavailable %s: ', url)
            raise ResourceUnavailable(str(e))
        dest = self._get_dest_for_url(url)
        with open(dest, 'wb') as fp:
            for chunk in r.iter_content(chunk_size=1024):
                fp.write(chunk)
        if metasearch:
            meta = self._get_meta_for_url(url)
            if meta:
                with open(dest + '.rbx.json', 'w') as fp:
                    to_json(meta, fp)
        return dest

    def _get_meta_for_url(self, url):
        log.debug('Fetching metadata for %s', url)
        meta_url = copy(url)
        meta_url.path = url.path + '.rbx.json'
        r = requests.get(meta_url.geturl())
        if not r.ok:
            log.warning('Failed to get metadata for URL %s', url)
            return
        try:
            meta = r.json()
            assert isinstance(meta, dict)
            log.info('Fetched metadata from %s', meta_url)
        except:
            log.warning('Metadata not valid JSON object: %s', meta_url)
            return
        return meta

    def _meta(self, path, prompt=True):
        file_meta = {}
        if isfile(path + '.meta'):
            with open(path + '.meta') as m:
                file_meta = json.load(m)
        elif prompt:
            file_meta = self._metadata_prompt(basename(path))
        return file_meta

    def _metadata_prompt(self, input, metadata=None):
        cont = ''
        if metadata is None:
            metadata = {}
            cont = 'n'
            # cont = raw_input('Metadata for file %s not found. '
            #                  'Do you want to set it manually? [Y/n] '
            #                  % input)
            # print(cont.__class__)
            # cont = cont.lower().strip()
        if cont == 'y' or cont == '':
            key = raw_input("Key: ")
            if key == '':
                return metadata
            value = raw_input("Value: ")
            if value == '':
                return metadata
            metadata[key] = value
            return self._metadata_prompt(input, metadata)
        return metadata

    def _get_secondary_files(self, secondaryFiles, input, autodetect=True,
                             prompt=True):

        def secondary_files_autodetect(path):
            log.info('Searching for additional files for file: %s', path)
            return [fn for fn in glob.glob(path + '.*')
                    if not (basename(fn).endswith('.meta') or
                            basename(fn).endswith('.rbx.json'))
                    ]

        secFiles = []
        if secondaryFiles:
            for n, sf in enumerate(secondaryFiles):
                path = sec_files_naming_conv(input.path, sf)
                log.info('Downloading: %s', path)
                secFiles.append(File(self._download(URL(path), metasearch=False)))
        if autodetect:
            if not secondaryFiles:
                secondaryFiles = []
            autodetected = secondary_files_autodetect(input.path)
            ad = []
            names = [basename(f) for f in secondaryFiles]
            for sf in autodetected:
                sf = sf.replace(input.path, '')
                if sf not in names:
                    ad.append(six.text_type(sf))
            if ad:
                cont = raw_input('Do you want to include autodetected '
                                 'additional files for file %s %s? [Y/n] '
                                 % (input, six.text_type(ad))).lower().strip()
                if cont == 'y' or cont == '':
                    log.info("Additional files %s included" % six.text_type(ad))
                    secFiles.extend(self._get_secondary_files(
                        ad, input, autodetect=False, prompt=False))
        if prompt:
            prompt = self._prompt_files(input, secFiles=secondaryFiles)
            secFiles.extend(self._get_secondary_files(
                prompt, input, autodetect=False, prompt=False))
        return secFiles

    def _get_dest_for_url(self, url):
        name = url.path.split('/')[-1]
        tgt = join(self.dir, name)
        if exists(tgt) or not name:
            return tempfile.mktemp(dir=self.dir)
        return tgt

    def _prompt_files(self, input, prompt=None, secFiles=None):

        cont = 'n'
        # cont = ''
        # if prompt is None:
        #     prompt = []
        #     cont = raw_input('Do you want to include some additional '
        #                      'files for file %s %s? [y/N] '
        #                      % (input, str(secFiles))).lower().strip()
        if cont == 'y':
            ext = raw_input('Extension: ')
            if ext == '':
                return prompt
            elif ext in secFiles:
                log.error('Extension %s already included' % ext)
            else:
                prompt.append(ext)
                secFiles.append(ext)
            return self._prompt_files(input, prompt, secFiles=secFiles)
        else:
            return prompt

    def _rbx_dump(self, input):
        # cont = raw_input(
        #     'Do you want to create rbx.json for file %s? [Y/n] '
        #     % input.path)
        #
        # cont = cont.decode('ascii').lower().strip()
        #
        cont = 'n'

        if cont == 'y' or cont == '':
            filename = input.path + '.rbx.json'
            with open(filename, 'w') as f:
                json.dump(input, f)
                log.info('File %s created.', filename)

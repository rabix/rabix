import os
import tempfile
import logging
import uuid
import copy
import json
import six
import requests
import glob

from six.moves.urllib import parse as urlparse
from six.moves import input as raw_input
from rabix.common.errors import ResourceUnavailable
from rabix.common.util import sec_files_naming_conv, url_type
from rabix.common.ref_resolver import from_url


log = logging.getLogger(__name__)


def to_json(obj, fp=None):
    default = lambda o: (o.__json__() if callable(getattr(o, '__json__',
                                                          None))
                         else six.text_type(o))
    kwargs = dict(default=default, indent=2, sort_keys=True)
    return json.dump(obj, fp, **kwargs) if fp else json.dumps(obj, **kwargs)


class InputCollector(object):
    """
    Will handle local files, 'data:,' URLs (for tests) and delegate other
    URLs to requests.get()
    """
    def __init__(self):
        self.downloader = self.detect_downloader()

    @staticmethod
    def detect_downloader():
        pass

    @property
    def task_dir(self):
        if not self.dir:
            self.dir = str(uuid.uuid4())
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)
        return self.dir

    def set_dir(self, job_dir):
        self.dir = job_dir

    def download(self, path, secondaryFiles=None, prompt=True):
        npath = self._download(path, metasearch=True)
        if os.path.exists(npath + '.rbx.json'):
            file = from_url(path + '.rbx.json')
            startdir = os.path.dirname(path)
            for i, v in enumerate(file.get('secondaryFiles', [])):
                spath = v['path']
                if not os.path.isabs(spath) and url_type(spath) == 'file':
                    spath = os.path.join(startdir, spath)
                file['secondaryFiles'][i]['path'] = self._download(
                    spath, metasearch=False)
            file['path'] = npath
        else:
            file = {}
            file['metadata'] = self._meta(path, prompt=prompt)
            if secondaryFiles:
                file['secondaryFiles'] = self._get_secondary_files(secondaryFiles,
                                                                   path,
                                                                   prompt=prompt)
            file['path'] = npath
            if prompt:
                self._rbx_dump(file)
        return file

    def _download(self, url, metasearch=True):
        if url.startswith('data:,'):
            return self._data_url(url)
        if '://' not in url:
            url = 'file://' + url
        if url.startswith('file://'):
            return self._local(url)

        log.debug('Downloading %s', url)
        r = requests.get(url)
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
        return os.path.abspath(dest)

    def _get_meta_for_url(self, url):
        log.debug('Fetching metadata for %s', url)
        chunks = list(urlparse.urlparse(url))
        chunks[2] += '.rbx.json'
        meta_url = urlparse.urlunparse(chunks)
        r = requests.get(meta_url)
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
        if os.path.exists(path + '.meta'):
            with open(path + '.meta') as m:
                file_meta = json.load(m)
        elif prompt:
            file_meta = self._metadata_prompt(os.path.basename(path))
        return file_meta

    def _metadata_prompt(self, input, metadata=None):
        cont = ''
        if metadata is None:
            metadata = {}
            cont = raw_input('Metadata for file %s not found. '
                             'Do you want to set it manually? [Y/n] '
                             % input).lower().strip()
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
                    if not (os.path.basename(fn).endswith('.meta') or
                            os.path.basename(fn).endswith('.rbx.json'))
                    ]

        secFiles = []
        if secondaryFiles:
            for n, sf in enumerate(secondaryFiles):
                path = sec_files_naming_conv(input, sf)
                log.info('Downloading: %s', path)
                secFiles.append({'path': self._download(
                    path, metasearch=False)})
        if autodetect:
            if not secondaryFiles:
                secondaryFiles = []
            autodetected = secondary_files_autodetect(input)
            ad = []
            names = [os.path.basename(f) for f in secondaryFiles]
            for sf in autodetected:
                sf = sf.replace(input, '')
                if sf not in names:
                    ad.append(str(sf))
            if ad:
                cont = raw_input('Do you want to include autodetected '
                                 'additional files for file %s %s? [Y/n] '
                                 % (input, str(ad))).lower().strip()
                if cont == 'y' or cont == '':
                    log.info("Additional files %s included" % str(ad))
                    secFiles.extend(self._get_secondary_files(
                        ad, input, autodetect=False, prompt=False))
        if prompt:
            prompt = self._prompt_files(input, secFiles=secondaryFiles)
            secFiles.extend(self._get_secondary_files(
                prompt, input, autodetect=False, prompt=False))
        return secFiles

    def _data_url(self, url):
        data = url[len('data:,'):]
        dest = tempfile.mktemp(dir=self.task_dir)
        with open(dest, 'w') as fp:
            fp.write(data)
        return os.path.abspath(dest)

    def _get_dest_for_url(self, url):
        path = urlparse.urlparse(url).path
        name = path.split('/')[-1]
        tgt = os.path.join(self.task_dir, name)
        if os.path.exists(tgt) or not name:
            return tempfile.mktemp(dir=self.task_dir)
        return tgt

    def _local(self, url):
        path = url[len('file://'):]
        if not os.path.isfile(path):
            raise ResourceUnavailable('Not a file: %s' % path)
        return os.path.abspath(path)

    def _prompt_files(self, input, prompt=None, secFiles=None):
        cont = ''
        if prompt is None:
            prompt = []
            cont = raw_input('Do you want to include some additional '
                             'files for file %s %s? [y/N] '
                             % (input, str(secFiles))).lower().strip()
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
        cont = raw_input('Do you want to create rbx.json for file %s? [Y/n] ' % input['path']).lower().strip()
        if cont == 'y' or cont == '':
            filename = input['path'] + '.rbx.json'
            with open(filename, 'w') as f:
                json.dump(input, f)
                log.info('File %s created.', filename)

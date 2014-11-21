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
from rabix.common.errors import ResourceUnavailable
from rabix.cliche.adapter import from_url

log = logging.getLogger(__name__)


def to_json(obj, fp=None):
    default = lambda o: (o.__json__() if callable(getattr(o, '__json__',
                                                          None))
                         else six.text_type(o))
    kwargs = dict(default=default, indent=2, sort_keys=True)
    return json.dump(obj, fp, **kwargs) if fp else json.dumps(obj, **kwargs)


class InputRunner(object):
    """
    Will handle local files, 'data:,' URLs (for tests) and delegate other
    URLs to requests.get()
    """
    def __init__(self, job, inputs, dir=None):
        self.inputs = inputs
        self.job = job
        self.dir = dir

    def __call__(self, *args, **kwargs):
        remaped_job = copy.deepcopy(self.job)
        input_values = self.job.get('inputs')
        if self.inputs:
            self._resolve(self.inputs, input_values, remaped_job['inputs'])
        return remaped_job

    def _resolve(self, inputs, input_values, remaped_job):
        is_single = lambda i: any([inputs[i]['type'] == 'directory',
                                   inputs[i]['type'] == 'file'])
        is_array = lambda i: inputs[i]['type'] == 'array' and any([
            inputs[i]['items']['type'] == 'directory',
            inputs[i]['items']['type'] == 'file'])
        is_object = lambda i: inputs[i]['type'] == 'array' and inputs[i]['items']['type'] == 'object'
        if inputs:
            single = filter(is_single, [i for i in inputs])
            lists = filter(is_array, [i for i in inputs])
            objects = filter(is_object, [i for i in inputs])
            for inp in single:
                self._resolve_single(inp, inputs[inp], input_values.get(
                    inp), remaped_job)
            for inp in lists:
                self._resolve_list(inp, self.inputs[inp], input_values.get(
                    inp), remaped_job)
            for obj in objects:
                if input_values.get(obj):
                    for num, o in enumerate(input_values[obj]):
                        self._resolve(inputs[obj]['items']['properties'], o,
                                      remaped_job[obj][num])

    def _resolve_single(self, inp, input, input_value, remaped_job):
        if input_value:
            if input_value['path'].endswith('.rbx.json'):
                remaped_job[inp] = self._resolve_rbx(input_value['path'])
            secondaryFiles = copy.deepcopy(input.get(
                'adapter', {}).get('secondaryFiles'))
            remaped_job[inp]['path'] = self._download(input_value['path'])
            remaped_job[inp]['meta'] = self._meta(input_value)
            secFiles = self._get_secondary_files(
                secondaryFiles, remaped_job[inp]['path'])
            if secFiles:
                remaped_job[inp]['secondaryFiles'] = secFiles

    def _resolve_list(self, inp, input, input_value, remaped_job):
        if input_value:
            secondaryFiles = copy.deepcopy(input.get(
                'adapter', {}).get('secondaryFiles'))
            for num, inv in enumerate(input_value):
                if input_value[num]['path'].endswith('.rbx.json'):
                    remaped_job[inp][num] = self._resolve_rbx(input_value[num]['path'])
                else:
                    remaped_job[inp][num]['path'] = self._download(
                        input_value[num]['path'])
                    remaped_job[inp][num]['meta'] = self._meta(inv)
                    secFiles = self._get_secondary_files(
                            secondaryFiles, input_value[num]['path'])
                    if secFiles:
                        remaped_job[inp][num]['secondaryFiles'] = secFiles

    def _resolve_rbx(self, rbx_file):
        rbx = from_url(rbx_file)
        rbx['path'] = self._download(rbx.get('path'))
        if rbx('secondaryFiles'):
            secFiles = self._get_secondary_files(
                rbx('secondaryFiles'), rbx['path'])
            if secFiles:
                rbx['secondaryFiles'] = secFiles


    def _prompt_file(self):
        pass


    @property
    def task_dir(self):
        if not self.dir:
            self.dir = str(uuid.uuid4())
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)
        return self.dir

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
            raise ResourceUnavailable(str(e))
        dest = self._get_dest_for_url(url)
        with open(dest, 'wb') as fp:
            for chunk in r.iter_content(chunk_size=1024):
                fp.write(chunk)
        if metasearch:
            meta = self._get_meta_for_url(url)
            if meta:
                with open(dest + '.meta', 'w') as fp:
                    to_json(meta, fp)
        return os.path.abspath(dest)

    def _get_meta_for_url(self, url):
        log.debug('Fetching metadata for %s', url)
        chunks = list(urlparse.urlparse(url))
        chunks[2] += '.meta'
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

    def _meta(self, input):
        if os.path.exists(input['path'] + '.meta'):
            with open(input['path'] + '.meta') as m:
                file_meta = json.load(m)
        else:
            file_meta = self._metadata_prompt()
        job_meta = input.get('meta', {})
        file_meta.update(job_meta)
        return file_meta

    def _metadata_prompt(self, metadata=None):
        if not metadata:
            metadata = {}
            cont = raw_input('Metadata not found. Do you want to set it manually? [Y/n]').lower().strip()
        else:
            cont = raw_input('Do you want to add another key? [Y/n]').lower().strip()
        if cont == 'y' or cont =='':
            key = raw_input("Key: ")
            value = raw_input("Value: ")
            metadata[key] = value
            self._metadata_prompt(metadata)
            return metadata
        elif cont == 'n':
            return metadata

    def _get_secondary_files(self, secondaryFiles, input):

        def get_file_path(path, ext):
            return ''.join([path, ext[1:]]) if ext.startswith('*') else \
                ''.join(['.'.join(path.split('.')[:-1]), ext])

        def secondary_files_autodetect(path):
            return [fn for fn in glob.glob(path + '.*')
                    if not os.path.basename(fn).endswith('.meta')]

        if secondaryFiles:
            for n, sf in enumerate(secondaryFiles):
                path = get_file_path(input, sf)
                log.info('Downloading: %s', path)
                self._download(path, metasearch=False)
                secondaryFiles[n] = path
        else:
            secondaryFiles = []
        autodetected = secondary_files_autodetect(input)
        for sf in autodetected:
            if sf not in secondaryFiles:
                secondaryFiles.append(sf)
        return secondaryFiles

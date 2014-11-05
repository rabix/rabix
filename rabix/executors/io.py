import os
import tempfile
import logging
import uuid
import copy
import json
import six
import requests

from six.moves.urllib import parse as urlparse
from rabix.common.errors import ResourceUnavailable

log = logging.getLogger(__name__)


def to_json(obj, fp=None):
    default = lambda o: (o.__json__() if callable(getattr(o, '__json__', None))
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
        is_single = lambda i: any([self.inputs[i]['type'] == 'directory',
                                   self.inputs[i]['type'] == 'file'])
        is_array = lambda i: self.inputs[i]['type'] == 'array' and any([
            self.inputs[i]['items']['type'] == 'directory',
            self.inputs[i]['items']['type'] == 'file'])
        input_values = self.job.get('inputs')
        if self.inputs:
            single = filter(is_single, [i for i in self.inputs])
            lists = filter(is_array, [i for i in self.inputs])
            for inp in single:
                secondaryFiles = self.inputs[inp].get('adapter', {}).get(
                    'secondaryFiles')
                remaped_job['inputs'][inp]['path'] = self._download(
                    input_values[inp]['path'])
                remaped_job['inputs'][inp]['meta'] = self._meta(
                    input_values[inp])
                self._get_secondary_files(secondaryFiles, input_values[
                    inp]['path'])
            for inp in lists:
                secondaryFiles = self.inputs[inp].get('adapter', {}).get(
                    'secondaryFiles')
                for num, inv in enumerate(input_values[inp]):
                    remaped_job['inputs'][inp][num]['path'] = self._download(
                        input_values[inp][num]['path'])
                    remaped_job['inputs'][inp][num]['meta'] = self._meta(inv)
                    self._get_secondary_files(secondaryFiles, input_values[
                        inp][num]['path'])
            return remaped_job

    @property
    def task_dir(self):
        if not self.dir:
            self.dir = str(uuid.uuid4())
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)
        return self.dir

    def _download(self, url):
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
        file_meta = {}
        if os.path.exists(input['path'] + '.meta'):
            with open(input['path'] + '.meta') as m:
                file_meta = json.load(m)
        job_meta = input.get('meta', {})
        file_meta.update(job_meta)
        return file_meta

    def _get_secondary_files(self, secondaryFiles, input):
        if secondaryFiles:
            for sf in secondaryFiles:
                log.info('Downloading: %s', self._secondary_file(input, sf))
                self._download(self._secondary_file(input, sf))

    def _secondary_file(self, path, ext):
        if ext.startswith('*'):
            return ''.join([path, ext[1:]])
        else:
            return ''.join(['.'.join(path.split('.')[:-1]), ext])

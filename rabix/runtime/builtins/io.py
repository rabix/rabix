import os
import tempfile
import logging

import requests

# noinspection PyUnresolvedReferences
from six.moves.urllib import parse as urlparse

from rabix.cliche.ref_resolver import to_json
from rabix.common.errors import ResourceUnavailable
from rabix.runtime.tasks import Runner

log = logging.getLogger(__name__)


class InputRunner(Runner):
    """
    Will handle local files and 'data:,' URLs (for tests).
    Other URLs are delegated to requests.get()
    """
    def __init__(self, task):
        super(InputRunner, self).__init__(task)
        self.urls = task.arguments or []
        if not isinstance(self.urls, list):
            self.urls = [self.urls]
        self._task_dir = None

    @property
    def task_dir(self):
        if not self._task_dir:
            os.mkdir(self.task.task_id)
            self._task_dir = self.task.task_id
        return self._task_dir

    def run(self):
        return [
            os.path.abspath(x)
            for x in (self._download(url) for url in self.urls)
        ]

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
            raise ResourceUnavailable(url, cause=e)
        dest = self._get_dest_for_url(url)
        with open(dest, 'wb') as fp:
            for chunk in r.iter_content(chunk_size=1024):
                fp.write(chunk)
        meta = self._get_meta_for_url(url)
        if meta:
            with open(dest + '.meta', 'w') as fp:
                to_json(meta, fp)
        return dest

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
        return dest

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
            raise ResourceUnavailable(url)
        if not os.path.abspath(path).startswith(os.path.abspath('.')):
            raise NotImplementedError(
                'File must be in current dir or subdirs. Got %s' % path
            )
        return path

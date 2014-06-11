import os
import stat
import tempfile
import urlparse
import logging

import docker
import requests

from rabix.runtime.dockr import Container, get_image
from rabix.common.protocol import Job, Outputs, BaseJob, JobError
from rabix.common.errors import ResourceUnavailable
from rabix.common.util import import_name
from rabix.runtime.apps import DockerApp, MockApp
from rabix.runtime import from_json, to_json

log = logging.getLogger(__name__)
MOUNT_POINT = '/rabix'


class BaseRunner(object):
    """
    Base class for runners. All should have the same constructor:
    :param app: An app object. It should be of type known to runner.
    :param job_id: ID of job.
    :param job_args: dict of job arguments.
    :param job_resources: rabix.common.protocol.Resources instance. Resources allocated to job.
    :param job_context: dict with additional information (can be platform specific).
    """
    def __init__(self, app, job_id, job_args, job_resources, job_context):
        self.app = app
        self.job_id = job_id
        self.job_args = job_args
        self.job_resources = job_resources
        self.job_context = job_context

    def run_and_wait(self, raise_errors=True):
        raise NotImplementedError('Override run_and_wait for basic blocking runs.')

    def __call__(self):
        return self.run_and_wait(raise_errors=True)

    @classmethod
    def transform_input(cls, inp):
        return inp

    @classmethod
    def transform_output(cls, out):
        return out


class DockerRunner(BaseRunner):
    """
    Runs docker apps. Instantiates a container from specified image, mounts the current directory and runs entry point.
    A directory is created for each job.
    """
    def __init__(self, app, job_id, job_args, job_resources, job_context):
        super(DockerRunner, self).__init__(app, job_id, job_args, job_resources, job_context)
        if not isinstance(app, DockerApp):
            raise TypeError('Can only run app/tool/docker.')
        docker_image_ref = app.image_ref
        self.image_repo = docker_image_ref.get('image_repo')
        self.image_tag = docker_image_ref.get('image_tag')
        if not self.image_repo or not self.image_tag:
            raise NotImplementedError('Currently, can only run images specified by repo+tag.')
        self.job = Job(self.app.wrapper_id, job_id=job_id, args=job_args, resources=job_resources, context=job_context)
        self.container = None

    def run_and_wait(self, raise_errors=True):
        docker_client = docker.Client()
        image = get_image(docker_client, self.image_repo, self.image_tag)
        self.container = Container(docker_client, image['Id'], mount_point=MOUNT_POINT)

        job_dir = self.job.job_id
        os.mkdir(job_dir)
        os.chmod(job_dir, os.stat(job_dir).st_mode | stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
        in_file, out_file = [os.path.join(job_dir, f) for f in '__in__.json', '__out__.json']
        with open(in_file, 'w') as fp:
            to_json(self.job, fp)
        self.container.run_job('__in__.json', '__out__.json', cwd=job_dir)
        if not os.path.isfile(out_file):
            raise JobError('Job failed.')
        with open(out_file) as fp:
            result = from_json(fp)
        if raise_errors and isinstance(result, Exception):
            raise result
        return result


class InputRunner(BaseRunner):
    """
    Runs input jobs. File paths are prefixed with '../' to be accessible from containers.
    Will also handle 'data:,' URLs (for tests) and delegate other URLs to requests.get()
    """
    def __init__(self, app, job_id, job_args, job_resources, job_context):
        super(InputRunner, self).__init__(app, job_id, job_args, job_resources, job_context)
        self._job_dir = None

    @property
    def job_dir(self):
        if not self._job_dir:
            os.mkdir(self.job_id)
            self._job_dir = self.job_id
        return self._job_dir

    def run_and_wait(self, raise_errors=True):
        urls = self.job_args.get('url', [])
        if isinstance(urls, basestring):
            urls = [urls]
        results = [self._download(url) for url in urls]
        return Outputs({'io': results})

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
        except Exception, e:
            raise ResourceUnavailable(str(e))
        dest = self._get_dest_for_url(url)
        with open(dest, 'wb') as fp:
            for chunk in r.iter_content(chunk_size=1024):
                fp.write(chunk)
        meta = self._get_meta_for_url(url)
        if meta:
            with open(dest + '.meta', 'w') as fp:
                to_json(meta, fp)
        return '../' + dest

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
        dest = tempfile.mktemp(dir=self.job_dir)
        with open(dest, 'w') as fp:
            fp.write(data)
        return '../' + dest

    def _get_dest_for_url(self, url):
        path = urlparse.urlparse(url).path
        name = path.split('/')[-1]
        tgt = os.path.join(self.job_dir, name)
        if os.path.exists(tgt) or not name:
            return tempfile.mktemp(dir=self.job_dir)
        return tgt

    def _local(self, url):
        path = url[len('file://'):]
        if path.startswith('/'):
            raise NotImplementedError('No absolute paths yet. Got %s' % path)
        if not os.path.isfile(path):
            raise ResourceUnavailable('Not a file: %s' % path)
        if not os.path.abspath(path).startswith(os.path.abspath('.')):
            raise NotImplementedError('File must be in current dir or subdirs. Got %s' % path)
        return '../' + path


class OutputRunner(BaseRunner):
    """
    Runs output jobs. Since results come in absolute paths from containers, it will strip the MOUNT_POINT prefix.
    """
    def run_and_wait(self, raise_errors=True):
        results = [self._unpack(path) for path in self.job_args.get('$inputs', {}).get('io', [])]
        return Outputs({'io': results})

    def _unpack(self, path):
        mnt_point = MOUNT_POINT if MOUNT_POINT.endswith('/') else MOUNT_POINT + '/'
        return path[len(mnt_point):] if path.startswith(mnt_point) else path


class MockRunner(BaseRunner):
    """
    Runs the app/mock/python jobs. A directory is created for each job.
    """
    def run_and_wait(self, raise_errors=True):
        func = import_name(self.app.importable)
        job = BaseJob(self.job_id, self.job_args, self.job_resources, self.job_context)
        cwd = os.path.abspath('.')
        job_dir = self.job_id
        os.mkdir(job_dir)
        os.chdir(job_dir)
        try:
            return func(job)
        finally:
            os.chdir(cwd)


RUNNER_MAP = {
    'input': InputRunner,
    'output': OutputRunner,
    MockApp: MockRunner,
    DockerApp: DockerRunner,
}

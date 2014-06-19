import re
import os
import six
import keyword

from rabix.runtime.models import App
from rabix.runtime.tasks import Runner
from rabix.common.util import import_name
from rabix.common.protocol import WrapperJob


class MockApp(App):
    TYPE = 'app/mock/python'

    importable = property(lambda self: self['importable'])

    def _validate(self):
        self._check_field('importable', six.string_types, null=False)
        chunks = self['importable'].split('.')
        assert len(chunks) > 1, 'importable cannot be a module'
        for chunk in chunks:
            assert not keyword.iskeyword(chunk), (
                '"%s" is a Python keyword' % chunk
            )
            assert re.match('^[A-Za-z_][A-Za-z0-9_]*$', chunk), (
                '"%s" is not a valid Python identifier' % chunk
            )


class MockRunner(Runner):
    """
    Runs the app/mock/python jobs. A directory is created for each job.
    """
    def run(self):
        func = import_name(self.task.app.importable)
        job = WrapperJob(None, self.task.task_id, self.task.arguments,
                         self.task.resources, None)
        cwd = os.path.abspath('.')
        task_dir = self.task.task_id
        os.mkdir(task_dir)
        os.chdir(task_dir)
        try:
            return func(job)
        finally:
            os.chdir(cwd)

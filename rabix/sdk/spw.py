import functools
import itertools
import operator

from rabix.common import six
from rabix.sdk.wrapper import Wrapper
from rabix.sdk import require


class SimpleParallelWrapper(Wrapper):
    for_each = '', ''

    def __init__(self, *args, **kwargs):
        super(SimpleParallelWrapper, self).__init__(*args, **kwargs)
        if not all(attr.list for attr in self.outputs):
            raise TypeError(
                'All outputs must be declared as lists when using SPW.'
            )

    def _entry_point(self):
        return self.job('split')

    @require(100, require.CPU_NEGLIGIBLE)
    def split(self):
        input_id, metadata_key = self.for_each
        inp = getattr(self.inputs, input_id)

        if metadata_key.startswith('params.'):
            metadata_key = getattr(self.params, metadata_key[len('params.'):])
        if metadata_key not in ('sample_group', 'sample', 'library',
                                'platform_unit', 'chunk', 'file', None):
            raise ValueError('Invalid metadata_key')

        groups = group_inputs(inp, metadata_key)
        reqs = self.get_requirements()
        jobs = [
            self.job('work', requirements=reqs,
                     args={'group_id': k, 'files': files})
            for k, files in six.iteritems(groups)
        ]
        return self.job('merge', args={'job_results': jobs})

    def work(self, _, files):
        # Remove files from input_id that are not in job
        inp = getattr(self.inputs, self.for_each[0])
        inp._values = [v for v in inp._values if v.file in files]

        # Call wrapper
        self.execute()
        self.outputs._save_meta()

        # Return dict as job result
        return self.outputs.__json__()

    @require(100, require.CPU_NEGLIGIBLE)
    def merge(self, job_results):
        for result in job_results:
            for output_id, files in six.iteritems(result):
                out = getattr(self.outputs, output_id)
                for file_path in files:
                    out.add_file(file_path)._load_meta()


S = '__!__'


def make_rg_id(metadata_key, io_obj):
    rg = [
        io_obj.meta.sample_group or '',
        io_obj.meta.sample or '',
        io_obj.meta.library or '',
        io_obj.meta.platform_unit or '',
        str(io_obj.meta.chunk) if io_obj.meta.chunk is not None else '',
    ]
    rg_map = {
        'sample': rg[:2],
        'library': rg[:3],
        'platform_unit': rg[:4],
        'chunk': rg[:5],
    }
    return (
        S.join(rg_map[metadata_key]) if metadata_key in rg_map
        else getattr(io_obj.meta, metadata_key)
    )


def group_inputs(inp, metadata_key):
    if str(metadata_key) == 'None':
        return {'': [f for f in inp]}
    if metadata_key == 'file':
        return {f: [f] for f in inp}
    key_getter = functools.partial(make_rg_id, metadata_key)
    files = sorted(inp, key=key_getter)
    return {
        key: [f.file for f in val]
        for key, val in itertools.groupby(files, key_getter)
    }


def match(io_obj, job_id):
    if has_blank_meta(io_obj):
        return False
    job_id = job_id or ''
    chunk_id = make_rg_id('chunk', io_obj)
    return zipmatch(chunk_id.split(S), job_id.split(S))


def rtrim_iterable(iterable_):
    """
    >>> rtrim_iterable([1, 2, 0, 0])
    [1, 2]
    """
    return list(reversed(list(itertools.dropwhile(operator.not_,
                                                  reversed(iterable_)))))


def zipmatch(a, b):
    """
    >>> zipmatch([1, 2, 0], [1, 2, 3])
    True
    >>> zipmatch([1, 2, 3], [1, 2, 3])
    True
    >>> zipmatch([1, 2, 3], [1, 2, 4])
    False
    """
    return all(x[0] == x[1] for x in zip(rtrim_iterable(a), rtrim_iterable(b)))


def has_blank_meta(io_obj):
    return make_rg_id('chunk', io_obj) == S.join(['', '', '', '', ''])

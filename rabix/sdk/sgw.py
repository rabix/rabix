from rabix.common.protocol import Resources
from rabix.sdk.wrapper import Wrapper, _get_method_requirements

_error = NotImplementedError(
    'Methods split, work and merge must be overridden.'
)


class ScatterGatherWrapper(Wrapper):
    def _entry_point(self):
        return self.job(method='_split',
                        requirements=self.get_split_requirements())

    def _split(self):
        args_list = self.split()
        jobs = [
            self.job(method='_work',
                     requirements=self.get_work_requirements(args),
                     args={'job': args})
            for args in args_list
        ]
        return self.job(
            method='_merge', requirements=self.get_merge_requirements(),
            args={'job_results': jobs}
        )

    def _work(self, job):
        return self.work(job)

    def _merge(self, job_results):
        return self.merge(job_results)

    def split(self):
        raise _error

    def work(self, job):
        raise _error

    def merge(self, job_results):
        raise _error

    def get_split_requirements(self):
        return _get_method_requirements(self, 'split') or Resources()

    # noinspection PyUnusedLocal
    def get_work_requirements(self, job):
        return _get_method_requirements(self, 'work') or Resources()

    def get_merge_requirements(self):
        return _get_method_requirements(self, 'merge') or Resources()

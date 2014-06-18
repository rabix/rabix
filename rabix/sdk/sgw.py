from rabix.sdk.wrapper import Wrapper, _get_method_requirements
from rabix.common.protocol import Resources

error = NotImplementedError('Methods split, work and merge must be overridden.')


class ScatterGatherWrapper(Wrapper):
    def _entry_point(self):
        return self.job('_split', requirements=self.get_split_requirements(), name='split')

    def _split(self):
        args_list = self.split()
        jobs = [self.job('_work', requirements=self.get_work_requirements(args),
                         name='work_%s' % ndx, args={'job': args})
                for ndx, args in enumerate(args_list)]
        return self.job('_merge', name='merge', requirements=self.get_merge_requirements(),
                        args={'job_results': jobs})

    def _work(self, job):
        return self.work(job)

    def _merge(self, job_results):
        return self.merge(job_results)

    def split(self):
        raise error

    def work(self, job):
        raise error

    def merge(self, job_results):
        raise error

    def get_split_requirements(self):
        return _get_method_requirements(self, 'split') or Resources()

    # noinspection PyUnusedLocal
    def get_work_requirements(self, job):
        return _get_method_requirements(self, 'work') or Resources()

    def get_merge_requirements(self):
        return _get_method_requirements(self, 'merge') or Resources()

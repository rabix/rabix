VERSION = '0.1.0'
CONFIG = {
    'scheduler': {
        'class': 'rabix.runtime.scheduler.BasicScheduler',
        'workers': {
            'InputTask': 'rabix.runtime.builtins.io.InputRunner',
            'OutputTask': 'rabix.runtime.tasks.Worker',
            'AppInstallTask': {
                'app/tool/docker': 'rabix.runtime.builtins.dockr.DockerAppInstaller',
                'app/mock/python': 'rabix.runtime.tasks.Worker'
            },
            'PipelineStepTask': {
                'app/tool/docker': 'rabix.runtime.builtins.dockr.DockerRunner',
                'app/mock/python': 'rabix.runtime.builtins.mocks.MockRunner'
            }
        },
        'ram_mb': 7 * 1024,
    }
}

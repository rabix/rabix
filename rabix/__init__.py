VERSION = '0.2.0'
CONFIG = {
    'engine': {
        'class': 'rabix.runtime.engine.MultiprocessingEngine',
        'ram_mb': 7 * 1024,
    },
    'runners': {
        'InputTask': 'rabix.runtime.builtins.io.InputRunner',
        'OutputTask': 'rabix.runtime.tasks.Runner',
        'AppInstallTask': {
            'app/tool/docker': 'rabix.runtime.builtins.dockr.'
                               'DockerAppInstaller',
            'app/mock/python': 'rabix.runtime.tasks.Runner'
        },
        'PipelineStepTask': {
            'app/tool/docker': 'rabix.runtime.builtins.dockr.DockerRunner',
            'app/mock/python': 'rabix.runtime.builtins.mocks.MockRunner'
        }
    },
    'docker': {
    },
}

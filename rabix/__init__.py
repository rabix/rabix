__version__ = '0.3.0'

CONFIG = {
    'engine': {
        'class': 'rabix.runtime.engine.base.MultiprocessingEngine',
        'ram_mb': 2*1024,
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
    'registry': {
        'HOST': 'localhost',
        'DEBUG': True,
        'SECRET_KEY': '',
        'GITHUB_CLIENT_ID': '',
        'GITHUB_CLIENT_SECRET': '',
        'GITHUB_CALLBACK_URL': '',
        'MOCK_USER': False,
    },
    'docker': {},
    'nodes': [{'node_id': 'some-id', 'ram_mb': 2*1024, 'cpu': 1}],
    'redis': {'host': 'localhost', 'port': 6379, 'password': None},
}

from rabix.common.util import update_config

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


def test_cfg_update():
    update_config(cfg=CONFIG, path="./rabix/tests/configs/config_new.json")
    assert CONFIG['a']['b']['c']['d']['e']['f'] == 2
    assert CONFIG['b']['c'] == [1, 2, 3, 4, 5]
    assert CONFIG['c'] == 3
    assert CONFIG['runners']['AppInstallTask'][
        'app/tool/docker'] == 'changed.AppInstallTask'
    assert CONFIG['runners']['AppInstallTask']['app/tool/newtype'][
        'app'] == 'new.AppTask'
    assert CONFIG['runners']['InputTask'] == 'changed.InputTask'
    assert CONFIG['runners']['OutputTask'] == 'changed.OutputTask'

from rabix.common.util import update_config
from rabix import CONFIG


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
import os
import yaml
from rabix.common.errors import RabixError


def yaml_load(path='./rabix.yaml'):
    if os.path.exists(path):
        with open(path) as cfg:
            config = yaml.load(cfg)
            return config
    else:
        raise RabixError('Config file %s not found!' % path)

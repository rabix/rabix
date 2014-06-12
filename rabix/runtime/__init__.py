from rabix.common.protocol import MAPPINGS
from rabix.common import from_url, from_json, to_json
from rabix.runtime.models import Pipeline, AppSchema
from rabix.runtime.builtins.dockr import DockerApp
from rabix.runtime.builtins.mocks import MockApp


MAPPINGS.update({
    'app/mock/python': MockApp,
    'app/pipeline': Pipeline,
    'app/tool/docker': DockerApp,
    'schema/app/sbgsdk': AppSchema,
})


# PyCharm and similar complain about unused imports. Following are exposed from this module to patch the MAPPINGS dict:
if False:
    _ = from_json, from_url, to_json

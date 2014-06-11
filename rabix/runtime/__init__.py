from rabix.common.protocol import MAPPINGS, from_url, from_json
from rabix.runtime.apps import MockApp, Pipeline, DockerApp, AppSchema


MAPPINGS.update({
    'app/mock/python': MockApp,
    'app/pipeline': Pipeline,
    'app/tool/docker': DockerApp,
    'schema/app/sbgsdk': AppSchema,
})


# PyCharm and similar complain about unused imports. Following are exposed from this module to patch the MAPPINGS dict:
if False:
    _ = from_json, from_url

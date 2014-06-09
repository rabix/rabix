from rabix.common.protocol import MAPPINGS
from rabix.runtime.apps import MockApp, Pipeline, DockerApp, AppSchema


MAPPINGS.update({
    'app/mock/python': MockApp,
    'app/pipeline': Pipeline,
    'app/tool/docker': DockerApp,
    'schema/app/sbgsdk': AppSchema,
})

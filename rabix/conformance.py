import json
from rabix.cli import CLIJob, CreateFileRequirement
from rabix.common.models import Job, get_inputs
from rabix.expressions import ValueResolver


def conformance_test(context, app, job_dict, basedir):
    job_dict['class'] = 'Job'
    job_dict['id'] = basedir
    job_dict['app'] = app

    if not app.outputs:
        app.outputs = []

    job_dict['inputs'] = get_inputs(job_dict, app.inputs, basedir)
    job = Job.from_dict(context, job_dict)

    adapter = CLIJob(job)

    result = {
        'args': adapter.make_arg_list(),
        'stdin': adapter.stdin,
        'stdout': adapter.stdout,
    }

    cfr = app.get_requirement(CreateFileRequirement)
    e = ValueResolver(job)

    if cfr:
        result['createfiles'] = {
            name: content
            for name, content in cfr.resolve_file_defs(e)
        }

    print(json.dumps(result))

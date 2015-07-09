import json
from flask import Flask, request
from rabix.cli.adapter import CLIJob
from rabix.common.models import Job, process_builder, construct_files
from rabix.common.context import Context
from rabix.cli import init


app = Flask(__name__)
ctx = Context(None)
init(ctx)


@app.route('/get_command_line', methods=['POST'])
def get_command_line():
    data = request.get_json(force=True)
    tool = process_builder(ctx, data['tool_cfg'])
    inputs = {k: construct_files(v, tool._inputs[k].validator) for k, v in data['input_map'].iteritems()}
    job = Job('Fake job ID', tool, inputs,  {'cpu': 1, 'mem': 1024}, ctx)
    cli_job = CLIJob(job)
    return json.dumps({
        'arguments': cli_job.make_arg_list(),
        'stdin': cli_job.stdin,
        'stdout': cli_job.stdout,
    }, indent=2)


@app.route('/get_outputs', methods=['POST'])
def get_outputs():
    data = request.get_json(force=True)
    tool = process_builder(ctx, data['tool_cfg'])
    inputs = {k: construct_files(v, tool._inputs[k].validator) for k, v in data['input_map'].iteritems()}
    job = Job('Fake job ID', tool, inputs,  {'cpu': 1, 'mem': 1024}, ctx)
    cli_job = CLIJob(job)
    status = 'SUCCESS' if data['exit_code'] in data['tool_cfg'].get('successCodes', [0]) else 'FAILURE'
    return json.dumps({
        'status': status,
        'outputs': cli_job.get_outputs(data['job_dir'], job),
    }, indent=2)


if __name__ == '__main__':
    app.run(debug=True)
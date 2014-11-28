import os


def validate_inputs(tool, job):
    required = tool.get('inputs', {}).get('required', [])
    inputs = job.get('inputs', None)
    for req in required:
        if req not in inputs.keys():
            raise Exception('Required input not set')
        else:
            if isinstance(inputs[req], list):
                for f in inputs[req]:
                    if not os.path.exists(f['path']):
                        raise Exception("File %s doesn't exist" % f['path'])
            else:
                if not os.path.exists(inputs[req]['path']):
                    raise Exception(
                        "File %s doesn't exist" % inputs[req]['path'])

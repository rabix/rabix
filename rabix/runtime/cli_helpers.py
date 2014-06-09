import os
import sys


def before_job(node):
    print 'Running step "%s". Job: "%s" ...' % (node.step['id'], node.node_id),
    sys.stdout.flush()


def after_job(_):
    print 'Done.'
    sys.stdout.flush()


def present_outputs(outputs):
    row_fmt = '{:<20}{:<80}{:>16}'
    print ''
    print row_fmt.format('Output ID', 'File path', 'File size')
    for out_id, file_list in outputs.iteritems():
        for path in file_list:
            print row_fmt.format(out_id, path, str(os.path.getsize(path)))

#!/usr/bin/python

import os
import argparse
import json
import datetime

CONTROL_PATH = "/tmp"
VALID_STATUS = ['OK','WARNING','ERROR']

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def parse_arguments():
    parser = argparse.ArgumentParser(
        description=("A script for publishing and maintaining job status files")
    )
    parser.add_argument("-j", "--jobname", required=True)
    parser.add_argument("-d", "--duration", type=int)
    parser.add_argument("-m", "--message")
    parser.add_argument("-s", "--status", required=True, choices=VALID_STATUS)
    parser.add_argument("-r", "--running", required=True, type=str2bool, default=False, choices=[True,False])

    # No dict comprehension in 2.6
    rv = {}
    for k, v in vars(parser.parse_args()).items():
        if v != None:
            rv.update({k: v})

    return rv

def update_status(running_pid, job_name, status, is_running, **kwargs):
    if status not in VALID_STATUS:
        raise Exception("Status must be one of: {0}".format(", ".join(VALID_STATUS)))

    jn = job_name.lower().replace(' ','_')
    job_file = "{0}/batch_job.{1}.json".format(CONTROL_PATH, jn)

    rv = {}
    if 'duration' in kwargs and kwargs.get('duration') > 0:
        rv.update({"duration": kwargs.get('duration')})

    if os.path.isfile(job_file):
        with open(job_file, 'r') as f:
            data = f.read()
        f.closed

        prev_values = json.loads(data)
        # it is unlikely that the pid and name will match on consecutive runs
        if prev_values.get('name') == job_name and prev_values.get('pid') == running_pid:
            rv.update(prev_values)

    fd = open(job_file, 'w')
    rv.update({ "pid": running_pid,
                "name": job_name,
                "status": status,
                "running": is_running })

    for extra_field in ['message','duration']:
        if extra_field in kwargs:
            rv.update({extra_field: kwargs[extra_field]})

    if not rv.get('started_at'):
        rv.update({"started_at": datetime.datetime.now().isoformat()})

    fd.write(json.dumps(rv))
    fd.close()

if __name__ == '__main__':
    args = parse_arguments()
    running_pid = os.getppid()

    update_status(running_pid,
                  args.pop('jobname'),
                  args.pop('status'),
                  args.pop('running'),
                  **args)

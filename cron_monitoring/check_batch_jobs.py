#!/usr/bin/python

import os
import stat
import re
import glob
import argparse
import json
import datetime

CONTROL_PATH = "/tmp"

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def parse_arguments():
    parser = argparse.ArgumentParser(
        description=("A script for returning details about jobs")
    )
    parser.add_argument("-c", "--count", action='store_true')
    parser.add_argument("-l", "--list", action='store_true')
    parser.add_argument("-j", "--jobname", type=str)
    parser.add_argument("-s", "--status", action='store_true')
    parser.add_argument("-u", "--update", action='store_true')
    parser.add_argument("-p", "--pidcheck", action='store_true')
    parser.add_argument("-d", "--durationcheck", action='store_true')

    # No dict comprehension in 2.6
    rv = {}
    for k, v in vars(parser.parse_args()).items():
        if v != None:
            rv.update({k: v})

    return rv

def get_job_list():
    jobs = []
    for p in glob.glob("{0}/batch_job.*.json".format(CONTROL_PATH)):
        with open(p, 'r') as f:
            content = f.read()
        f.closed

        data = json.loads(content)

        jobs.append({"{#JOBNAME}": data.get("name","UNKNOWN")})

    return jobs

if __name__ == '__main__':
    args = parse_arguments()
    if args.get('list'):
        print(json.dumps({"data": get_job_list()}, separators=(',',':')))

    elif args.get('count'):
        print(len(get_job_list()))

    elif args.get('status'):
        if not args.get('jobname'):
            raise Exception("A valid job name must be provided")

        jn = args.get('jobname').lower().replace(' ','_')
        fn = "{0}/batch_job.{1}.json".format(CONTROL_PATH, jn)
        if not os.path.isfile(fn):
            # raise Exception("Could not find control file for {0}".format(args.get('jobname')))
            print("")
        else:
            with open(fn, 'r') as f:
                data = f.read()
            f.closed

            content = json.loads(data)
            print(content.get('status','UNKNOWN'))

    elif args.get('update'):
        if not args.get('jobname'):
            raise Exception("A valid job name must be provided")

        jn = args.get('jobname').lower().replace(' ','_')
        fn = "{0}/batch_job.{1}.json".format(CONTROL_PATH, jn)
        if os.path.isfile(fn):
            print(os.stat(fn).st_mtime)

    elif args.get('pidcheck'):
        if not args.get('jobname'):
            raise Exception("A valid job name must be provided")

        rv = 0

        jn = args.get('jobname').lower().replace(' ','_')
        fn = "{0}/batch_job.{1}.json".format(CONTROL_PATH, jn)
        if os.path.isfile(fn):
            with open(fn, 'r') as f:
                data = f.read()
            f.closed

            content = json.loads(data)

            if content.get("running") and content.get('pid') and not os.path.isdir("/proc/{0}".format(content.get('pid'))):
                rv = content.get('pid')

            elif not content.get("running"):
                rv = -1

            print(rv)

    elif args.get('durationcheck'):
        if not args.get('jobname'):
            raise Exception("A valid job name must be provided")

        jn = args.get('jobname').lower().replace(' ','_')
        fn = "{0}/batch_job.{1}.json".format(CONTROL_PATH, jn)
        if os.path.isfile(fn):
            with open(fn, 'r') as f:
                data = f.read()
            f.closed

            content = json.loads(data)
            duration = content.get('duration',0)

            if duration > 0 and content.get('running'):
                sd = datetime.datetime.strptime(content.get('started_at'), "%Y-%m-%dT%H:%M:%S.%f") + datetime.timedelta(minutes=duration)
                diff = (datetime.datetime.now() - sd).total_seconds()

            else:
                diff = 0.0

            print(diff)

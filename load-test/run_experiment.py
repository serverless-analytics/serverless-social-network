#!/usr/bin/env python
import argparse
import socket
import random
import subprocess as sp
import time
import json
import requests
import sys_utils as utils
import os
from pathlib import Path
import yaml
import social_network as sn 

class Args:
    class load_db:
        def __init__(self):
            self.exp = None
            pass

    class locust:
        def __init__(self, confs):
            self.trace = confs['trace']
            self.logdir = confs['logdir']
            self.logfile = os.path.join(self.logdir, self.trace)
            self.load_database = False
            self.drop_all_dbs=False
            self.except_social_graph=True
            self.parallelism = 1
            self.duration = confs['duration']
            self.s = confs['s']
            self.n_interactions = confs['n_interactions']
            self.n_users = confs['n_users']
            print(self.trace, self.logdir)
            pass

    class replay:
        def __init__(self, confs):
            self.trace = confs['trace']
            self.logdir = confs['logdir']
            self.logfile = None
            self.load_database = False
            self.drop_all_dbs=False
            self.except_social_graph=True
            self.parallelism = 1
            print(self.trace, self.logdir)
            pass

    def __init__(self, fpath):   
        with open(fpath, 'r') as fd:
            t_conf = yaml.load(fd, Loader=yaml.FullLoader)
            self.n_workers = t_conf['cluster']['n-workers']
            self.cluster_membership = t_conf['cluster']['membership']
            self.mem_size = t_conf['cluster']['mem-size']
            self.policy = t_conf['cluster']['policy']
            self.mode = t_conf['cluster'].get('mode', 'local')
            self.exp_type = list(t_conf['experiment'].keys())[0]
            
            if 'replay' in t_conf['experiment']:
                self.exp = self.replay(t_conf['experiment']['replay'])
                self.exp.parallelism = 3*int(self.n_workers)
                print('Parallelism is ', self.exp.parallelism)
                self.exp.logfile = os.path.join(self.exp.logdir, f'{self.exp.trace.rsplit(".", 1)[0]}.{self.n_workers}.{self.policy}.json')
                pass
            elif 'locust' in t_conf['experiment']:
                self.exp = self.locust(t_conf['experiment']['locust'])
                self.exp.parallelism = 2*self.n_workers
                pass
            elif 'load-db' in t_conf['experiment']:
                pass
            else:
                raise NameError('Operation is not supported')

            if 'remote' == t_conf['cluster']['mode']:
                self.mongo_client = "rfonseca-dask.westus2.cloudapp.azure.com"
            elif 'local' == t_conf['cluster']['mode']:
                self.mongo_client = socket.gethostname()


    def sanity_check_args(self):
        pass


def reset_caches(args):
    reset_cache_script=os.path.join(os.getcwd(), 'reset_cache.py')
    proc = sp.Popen(['python', reset_cache_script, 
        '--n-caches', str(args.n_workers),
        '--cluster-members', args.cluster_membership,
        '--cache-size', str(args.mem_size)])
    proc.wait()
    pass


def reset_load_balancer(args):
    load_balancer_script = os.path.join(Path(os.getcwd()).parent.absolute(), 'load-balancer/app.py')

    proc = sp.run("ps ax | grep app.py | awk 'NR==1{print $1}'", shell=True, stdout=sp.PIPE)
    pid = proc.stdout.decode()
    proc = sp.run(f'sudo kill -9 {pid}', shell=True) 

    proc = sp.Popen(['python', f'{load_balancer_script}',
        '--n-workers', str(args.n_workers),
        '--cluster-members', args.cluster_membership,
        '--port', str(7071),
        '--policy', args.policy,
        '--worker-mem-size', str(args.mem_size)])
    return proc


def get_args():
    parser = argparse.ArgumentParser(prog='run_experiment.py',
            description='Run the tpch queries')
    parser.add_argument('--config', 
            help='points to config file, if set all other optionals will be ignored',
            required=True,
            default=None)
    return parser.parse_args()


if __name__=="__main__":
    conf = get_args()
    args = Args(conf.config)
    if args.mode != 'local': reset_caches(args)
    #reset_load_balancer(args)
    sn.run(args)

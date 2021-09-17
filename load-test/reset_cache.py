#!/usr/bin/env python
import argparse
import random
import subprocess as sp
import time
import json
import requests
import sys_utils as utils
import os


def get_args():
    parser = argparse.ArgumentParser(prog='run_experiment.py',
            description='Run the tpch queries')
    parser.add_argument('--config', 
            help='points to config file, if set all other optionals will be ignored',
            required=False,
            default=None)
    
    parser.add_argument('--n-caches', 
            help='# of caches participates in this experiment',
            required=False,
            type=int,
            default=1)

    parser.add_argument('--cache-size',
            help='size of data assigned to each cache',
            required=False,
            type=int,
            default=8000000000)


    parser.add_argument('--cache-members', 
            help='points to config file that consist of cache memebrship list',
            required=False,
            default=None)
    
    parser.add_argument('--policy', 
            help='The policy is either virt_node (virtual nodes) or\
                    chain_color (chain coloring) policy',
            required=False,
            default='virt_node')
    
    parser.add_argument('--scheduler', 
            help='the serverless framework scheduler policy and identifies how\
                    function are distributed among different workers.\
                    The values are: [color_round_robin, server_round_robin,\
                    consistent_hashing, random, per_invocation].',
            required=False,
            default='random')

    parser.add_argument('--ndelegate', 
            help='Identifies the number of delegate workers\n\
                  This should be set only if the policy is set to virt_node.',
            required=False,
            type=int,
            default=1)
    
    parser.add_argument('--serialization', 
            help='The serialization policy, values could be [arrow, dill, persist].',
            required=False,
            default='persist')

    parser.add_argument('--queries', 
            help='A list of queries between [1 .. 22].',
            action='append',
            required=False,
            type=str,
            default=[])
    
    parser.add_argument('--repeat', 
            help='# of execution per benchmark',
            required=False,
            type=int,
            default=10)
    
    parser.add_argument('--data-locality', 
            help='The locality policy, this could be set to [pull, push]',
            required=False,
            default='pull')
    
    parser.add_argument('--venv-dir', 
            help='The directory for the environment variable',
            required=False,
            default='../.venv37')
    
    parser.add_argument('--dask-dir', 
            help='The directory for the dask variable',
            required=False,
            default='..')
    
    parser.add_argument('--cache-sync', 
            help='identifies whether the data should be written sync or async to the home location.\
                    if the data-locality is pull, this must be set to sync. \
                    if the data-locality is push, this could sync or async.',
            required=False,
            default='sync')
    
    parser.add_argument('--backend-sync', 
            help='Identifies the policy for writing the data to the backend storage.\
                    it is set to async by default.',
            required=False,
            default='async')
    
    parser.add_argument('--shotgun-factor', 
            help='The shotun factor',
            required=False,
            type=int,
            default=1)
    
    parser.add_argument('--dask-verbose-dir', 
            help='The verbose directory for dask to dump the executions statistics',
            required=False,
            default='/tmp/dask')
    
    return parser.parse_args()


def parse_config_file(args):
    import yaml
    fpath = args.config
    with open(fpath, 'r') as fd:
        t_conf = yaml.load(fd, Loader=yaml.FullLoader)
        if 'cache-members' in t_conf:
            args.cache_members = t_conf['cache-members']
        if 'scheduler' in t_conf:
            args.scheduler = t_conf['scheduler']
        if 'policy' in t_conf:
            args.policy = t_conf['policy']
        if 'serialization' in t_conf:
            args.serialization = t_conf['serialization']
        if 'data-locality' in t_conf:
            args.data_locality = t_conf['data-locality']
        if 'ndelegate' in t_conf:
            args.ndelegate = t_conf['ndelegate']
        if 'n-caches' in t_conf:
            args.n_caches = t_conf['n-caches']
        if 'query' in t_conf:
            args.query = t_conf['query']
    return args



def sanity_check_args(args):
    assert(args.cache_members)
    #print(args.ndelegate, type(args.ndelegate))
    #assert(not ((args.policy == 'chain_color') & (args.ndelegate == 1)))



def run(args):
    print(f'args are {args}')
    utils.restart_caches(args)
    utils.dump_cache_list(args)




if __name__=="__main__":
    args = get_args()
    if args.config:
        args = parse_config_file(args)
    run(args)

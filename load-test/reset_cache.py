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


    parser.add_argument('--cluster-members', 
            help='points to config file that consist of cache memebrship list',
            required=False,
            default=None)
    
    
    return parser.parse_args()


def parse_config_file(args):
    import yaml
    fpath = args.config
    with open(fpath, 'r') as fd:
        t_conf = yaml.load(fd, Loader=yaml.FullLoader)
        print(t_conf)
        if 'cluster-members' in t_conf:
            args.cluster_members = t_conf['cluster-members']
        if 'n-caches' in t_conf:
            args.n_caches = t_conf['n-caches']
    return args



def sanity_check_args(args):
    assert(args.cache_members)



def run(args):
    print(f'args are {args}')
    utils.restart_caches(args)
    utils.dump_cache_list(args)


if __name__=="__main__":
    args = get_args()
    if args.config:
        args = parse_config_file(args)
    run(args)

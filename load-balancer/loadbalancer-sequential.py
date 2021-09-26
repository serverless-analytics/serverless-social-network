#!/usr/bin/env python
import argparse
import requests
import random
from consistent_hash import ConsistentHash
from crr import ColorRoundRobin

from flask import Flask,request,redirect,Response
from colorama import Fore, Style


app = Flask(__name__)
azure_func_host = 'http://localhost:5000'

servers = None
ch_ring = None
crr_ring = None
server_names = None

def get_args():
    parser = argparse.ArgumentParser(prog='loadbalancer.py',
            description='Run the http load balancer for Azure functions')
    parser.add_argument('--config', 
            help='points to config file, if set all other optionals will be ignored',
            required=False,
            default=None)
    
    parser.add_argument('--n-workers', 
            help='# of caches participates in this experiment',
            required=False,
            type=int,
            default=1)

    parser.add_argument('--worker-mem-size',
            help='size of data assigned to each cache',
            required=False,
            type=int,
            default=8000000000)

    parser.add_argument('--cluster-members', 
            help='points to config file that consist of cache memebrship list',
            required=False,
            default=None)
    
    parser.add_argument('--policy', 
            help='The policy is color round robin (crr), consistent hashing (ch), random (random), round robin (rr)',
            required=False,
            default='crr')

    return parser.parse_args()



def parse_configs(args):
    import yaml
    fpath = args.config
    with open(fpath, 'r') as fd:
        t_conf = yaml.load(fd, Loader=yaml.FullLoader)
        if 'cluster-members' in t_conf:
            args.cluster_members = t_conf['cluster-members']
        if 'policy' in t_conf:
            args.policy = t_conf['policy']
        if 'n-workers' in t_conf:
            args.n_workers = t_conf['n-workers']
        if 'worker-mem-size' in t_conf:
            args.worker_mem_size = t_conf['worker-mem-size']
    return args



def init_config(args):
    global servers; 
    global server_names;
    global ch_ring
    global crr_ring
    def get_servers(args):
        import json
        with open(args.cluster_members, 'r') as fd:
            t_members = json.load(fd)['CacheList']
        return t_members[0:int(args.n_workers)]
    servers = get_servers(args)
    server_names = [s['address'] for s in servers]
    if args.policy == 'ch':
        ch_ring = ConsistentHash(server_names, replicas=100)
    elif args.policy == 'crr':
        crr_ring = ColorRoundRobin(server_names)



def select_server(policy, locality=None):
    global servers
    global ch_ring
    global server_names

    if policy == 'random':
        assert(server_names)
        return random.choice(server_names)
    if policy == 'ch':
        assert(ch_ring)
        return ch_ring.get_member(locality)
    if policy == 'crr':
        assert(crr_ring)
        return crr_ring.get_member(locality) 



def distribute_load(params):
    global args
    for p in params:
        if p[0] == 'locality':
            return select_server(args.policy, locality=p[1])
    return select_server(policy='random') # select a server randomly. You need an error mesg which says you swithced to rnadom. 




@app.route('/')
def index():
    return 'Flask is running!'



@app.route('/<path:path>', methods=['GET','POST', 'PUT'])
def proxy(path):
    
    headers = [(name, value) for (name, value) in request.headers.items()]
    params = [(name, value) for (name, value) in request.args.items()]
    print(f'{Fore.GREEN} path: {path}, params: {params}, headers: {headers} {Style.RESET_ALL}')
    azure_func_host = distribute_load(params)
    
    if request.method=='GET':
        resp = requests.get(f'{azure_func_host}/{path}')
        excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        headers = [(name, value) for (name, value) in  resp.raw.headers.items() if name.lower() not in excluded_headers]
        response = Response(resp.content, resp.status_code, headers)
        return response
    elif request.method=='POST':
        resp = requests.post(f'{azure_func_host}/{path}',json=request.get_json())
        excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        headers = [(name, value) for (name, value) in resp.raw.headers.items() if name.lower() not in excluded_headers]
        response = Response(resp.content, resp.status_code, headers)
        return response
    elif request.method=='PUT':
        resp = requests.put(f'{azure_func_host}/{path}',json=request.get_json())
        excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        headers = [(name, value) for (name, value) in resp.raw.headers.items() if name.lower() not in excluded_headers]
        response = Response(resp.content, resp.status_code, headers)
        return response




if __name__ == '__main__':
    args = get_args()
    if args.config:
        args = parse_configs(args)
    init_config(args)
    app.run(debug = True,port=7073)

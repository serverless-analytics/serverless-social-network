#!/usr/bin/env python
import argparse
import requests
import random

from flask import Flask,request,redirect,Response
from colorama import Fore, Style

from aiohttp import web
import asyncio
import aiohttp

from funcproxy import LoadBalancer


app = Flask(__name__)
azure_func_host = 'http://localhost:5000'


def get_args():
    parser = argparse.ArgumentParser(prog='loadbalancer.py',
            description='Run the http load balancer for Azure functions')
    parser.add_argument('--config', 
            help='points to config file, if set all other optionals will be ignored',
            required=False,
            default=None)
    
    parser.add_argument('--port', 
            help='The port number, the load-balancer listens to',
            required=False,
            type=int,
            default=7071)
    
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
        if 'port' in t_conf:
            args.port = t_conf['port']
    return args



async def handle_get(request, host):
    params = await request.json()
    url_params =dict(request.query) 
    print(f'inside handle_post request {host}/{request.path},\n headers \
            {request.headers},\n url is {request.url},\n query is {dict(request.query)}\n \
            json is {params.keys()}')
   
    async with aiohttp.ClientSession() as session:
        async with session.get(f'{host}{request.path}', 
                json=params,
                params = url_params) as resp:
            print(f'status is {resp.status}, response {(await resp.json()).keys()}')
            return web.Response(text = (await resp.text()))


async def handle_put(request, host):
    params = await request.json()
    url_params =dict(request.query) 
    print(f'inside handle_post request {host}/{request.path},\n headers \
            {request.headers},\n url is {request.url},\n query is {dict(request.query)}\n \
            json is {params.keys()}')
   
    async with aiohttp.ClientSession() as session:
        async with session.put(f'{host}{request.path}', 
                json=params,
                params = url_params) as resp:
            print(f'status is {resp.status}, response {(await resp.json()).keys()}')
            return web.Response(text = (await resp.text()))


async def handle_post(request, host):
    params = await request.json()
    url_params =dict(request.query) 
    print(f'POST request {host}/{request.path},\n headers \
            {request.headers},\n url is {request.url},\n query is {dict(request.query)}\n \
            json is {params.keys()}')
   
    async with aiohttp.ClientSession() as session:
        async with session.post(f'{host}{request.path}', 
                json=params,
                params = url_params) as resp:
            print(f'status is {resp.status}, response {(await resp.json()).keys()}')
            return web.Response(text = (await resp.text()))


async def handle(request):
    name = request.match_info.get('name', "Anonymous")
    text = "Hello, " + name
    return web.Response(text=text)


app = web.Application(client_max_size=1024**3)
if __name__ == '__main__':
    args = get_args()
    if args.config:
        args = parse_configs(args)
    print(args)
    proxy = LoadBalancer(args)
    proxy.reg_method('GET', handle_get)
    proxy.reg_method('PUT', handle_put)
    proxy.reg_method('POST', handle_post)
    #app.add_routes([web.get('/<path:path>', proxy.do_route),
    #            web.put('/<path:path>', proxy.do_route),
    #            web.post('/<path:path>', proxy.do_route)])
    app.add_routes([web.route('*', '/home', handle)])
    app.add_routes([web.route('*', '/{tail:.*}', proxy.do_route)])
    #app.add_routes('*', '/home', handle)
    #app.router.add_route('*', '/{tail:.*}', proxy.do_route)
    web.run_app(app, port=args.port)

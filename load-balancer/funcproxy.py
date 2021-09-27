import logging
import aiohttp
import asyncio
import random
import json

from consistent_hash import ConsistentHash
from crr import ColorRoundRobin

from threading import Thread

class LoadBalancer:
    def __init__(self, args):
        self._handlers = {}
        self.n_workers = args.n_workers
        self.servers = self.init_workers(args.cluster_members)
        self.server_names = [s['address'] for s in self.servers]
        self.policy = args.policy 
        self.ch_ring = ConsistentHash(self.server_names, replicas=100) if self.policy == 'ch' else None
        self.crr_ring = ColorRoundRobin(self.server_names) if self.policy == 'crr' else None
        self.lock = asyncio.Lock() if self.policy == 'crr' else None
        self.threads = []

    def init_workers(self, fworkers_fpath):
        with open(fworkers_fpath, 'r') as fd:
            t_members = json.load(fd)['CacheList']
        return t_members[0:int(self.n_workers)]


    async def select_server(self, policy, locality=None):
        if policy == 'random':
            assert(self.server_names)
            return random.choice(self.server_names)
        if policy == 'ch':
            assert(self.ch_ring)
            return ch_ring.get_member(locality)
        if policy == 'crr':
            assert(self.crr_ring)
            async with self.lock:
                worker = crr_ring.get_member(locality) 
            return worker 
        
    async def distribute_load(self, params):
        for p, val in params:
            if p == 'locality':
                logging.warning(f'distributed_load, policy is {crr}, locality is {val}')
                return (await self.select_server(self.policy, locality=val))
        return (await self.select_server(policy='random')) # select a server randomly. You need an error mesg which says you swithced to rnadom. 

    async def do_route(self, request):
        logging.info(f'recieves {request.headers}')
        headers = [(name, value) for (name, value) in request.headers.items()]
        params = dict(request.query) #[(name, value) for (name, value) in request.args.items()]
        host = (await self.distribute_load(headers))
        
        logging.info(f'path: {request.url}, params: {params}, headers: {headers}, route to: {host}')
        method = self._handlers.get(request.method)
        if method is not None:
            #t = Thread(target = self.handle_request, args=(method, request, host))
            #t.start()
            #self.threads.append(t)
            #return
            return (await method(request, host))
        raise HTTPNotAcceptable()

    async def handle_request(self, method, request, host):
        return (await method(request, host))

    def reg_method(self, method, handler):
        self._handlers[method] = handler


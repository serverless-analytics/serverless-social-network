import gevent  # isort:skip
import gevent.monkey  # isort:skip
gevent.monkey.patch_all()  # isort:skip

import random
import socket
import string
import sys
import urllib
import zipfile
from distutils.dir_util import copy_tree
from pathlib import Path

import docker
import numpy as np
import pymongo
import urllib3
from pymongo import MongoClient
from tqdm import tqdm

from locust import HttpUser, between, events, task
from locust.env import Environment
from locust.log import setup_logging
from locust.stats import stats_printer#, write_csv_files
from utils.action import (create_action, create_sequence, invoke_action,
                          update_action_limits)
from utils.activation import get_activations
from utils.config import (ACCESS_KEY, APIHOST, AUTH_KEY, BUCKET, NAMESPACE,
                          SECRET_KEY, USER_PASS)
from utils.docker_image import docker_image_build, docker_image_push
from utils.init_config import init_config
from utils.logger import get_logger



import json
import ast
import itertools
import math
import threading
from queue import Queue

# -----------------------------------------------------------------------
# Global variables
# -----------------------------------------------------------------------

class Replay:
    def __init__(self, args, dbs, logger = None):
        self.dbs = dbs
        self.logger = logger
        self.args = args
        self.n_users = 63392
        self.replay_trace_queue = Queue()
        self._medias = set()
        self.concurrent_req = threading.Semaphore(args.exp.parallelism)        
        self.broken_requests = 0
        self.executed_requests = 0

    def compose_post(self, request):
        user_id = request['user_id']
        username = 'username_' + str(user_id)
        text = ''.join(random.choices(
            string.ascii_letters + string.digits, k=request['text_size']))
        num_user_mentions = len(request['user_mention_ids'])
        user_mention_ids = request['user_mention_ids']
    
        for user_mention_id in user_mention_ids:
            text = text + ' @username_' + str(user_mention_id)
        num_medias = len(request['media_ids'])
        media_ids = list()
        media_types = list()
        media_sizes = list()
        media_contents = list()
        for i in range(num_medias):
            while True:
                media_id = random.randint(1, sys.maxsize)
                if media_id not in self._medias:
                    self._medias.add(media_id)
                    break
    
            m_size = request['media_sizes'][i]
            media_ids.append(media_id)
            media_types.append('PIC')
            media_sizes.append(m_size)
            media_contents.append(''.join(random.choices(
                string.ascii_letters + string.digits, k=m_size)))
    
    
        action_name = 'compose_post'
        action_params = {
            'compose_post': {
                'username': username,
                'user_id': user_id,
                'text': text,
                'media_ids': media_ids,
                'media_types': media_types,
                'media_sizes': media_sizes,
                'media_contents': media_contents,
                'post_type': 'POST',
                'dbs': self.dbs
            }
        }
        res = invoke_action(action_name='compose_post',
                params=action_params, blocking=True, result=True)
        self.concurrent_req.release()
        return
        
    def read_home_timeline(self, request):
        user_id = request['user_id']
        start = request['start']
        stop = request['stop']
        request_id = request["request_id"]
        action_params = {
            'request_id': request_id,
            'read_home_timeline': {
                'user_id': user_id,
                'start': start,
                'stop': stop,
                'dbs': self.dbs
            }
        }
        print('inside read_home_timeline')
        ret = invoke_action(action_name='read_home_timeline_pipeline',
                params=action_params, blocking=True, result=True)
    
        #if isinstance(result, str):
        #    self.logger.critical(f'broken: read_home_timeline: req id {self.executed_requests}, {action_params}')
        #    self.executed_requests += 1
        #    self.broken_requests += 1
        #    self.concurrent_req.release()
        #    return
        try:
            result = json.loads(ret)
   
            trace_data = request
        
            objects = []
            cache_stats = []
            object_access = []
            timestamps = result['timestamps']
            post_ids = result['post_ids']
            for ps in result['posts']:
                cache_stats.append(ps['cache_status'])
                object_access.append(ps['object_access'])
                self.logger.info(ps['cache_status'])
    
            posts = [post  for p in result.get('posts', []) for post in p['posts']]
            for i, post in enumerate(posts):
                #self.logger.critical(f'oid: {post["post_id"]}, type: text, size: {len(post["text"])}. post: {post.keys()}')
                objects.append({'oid': post['post_id'], 'type': 'text', 'size': len(post['text']), 'author': post['author']['user_id']})
                medias = post['medias']
                for media in medias:
                    objects.append({'oid': media['media_id'], 'type': media['media_type'], 'size': media['media_size'], 'author': media['author']['user_id']})
                    #self.logger.critical(f'oid: {media["media_id"]}, type: {media["media_type"]}, size: {media["media_size"]}, author: {media["author"]["user_id"]}')
            trace_data['objects'] = objects
            trace_data['cache_status'] = cache_stats
            trace_data['object_access'] = object_access
            trace_data['timestamps'] = timestamps
            self.replay_trace_queue.put(trace_data)
        except Exception as ex:
             print(ret)
             print(type(ex).__name__)
             print(request)
             self.broken_requests += 1
             #raise NameError('Lets investigate the problem')

        self.logger.info(f'read_home_timeline: # of requests {self.executed_requests}, # of broken {self.broken_requests}')
        self.executed_requests += 1
        self.concurrent_req.release()
        return
    
    def read_user_timeline(self, request):
        user_id = request['user_id']
        start = request['start']
        stop = request['stop']
        request_id = request["request_id"]
        action_params = {
            'request_id': request_id,
            'read_user_timeline': {
                'user_id': user_id,
                'start': start,
                'stop': stop,
                'dbs': self.dbs
            }
        }
        print('inside read_user_timeline')
        ret = invoke_action(action_name='read_user_timeline_pipeline',
                params=action_params, blocking=True, result=True)
        
        #if True:
        try:
            result = json.loads(ret)
            trace_data = request
            
            objects = []
            cache_stats = []
            object_access = []
            timestamps = result['timestamps']
            post_ids = result['post_ids']
            for ps in result['posts']:
                cache_stats.append(ps['cache_status'])
                object_access.append(ps['object_access'])
                self.logger.info(ps['cache_status'])
        
            posts = [post  for p in result.get('posts', []) for post in p['posts']]
            for i, post in enumerate(posts):
                #self.logger.critical(f'oid: {post["post_id"]}, type: text, size: {len(post["text"])}. post: {post.keys()}')
                objects.append({'oid': post['post_id'], 'type': 'text', 'size': len(post['text']), 'author': post['author']['user_id']})
                medias = post['medias']
                for media in medias:
                    objects.append({'oid': media['media_id'], 'type': media['media_type'], 'size': media['media_size'], 'author': media['author']['user_id']})
                    #self.logger.critical(f'oid: {media["media_id"]}, type: {media["media_type"]}, size: {media["media_size"]}, author: {media["author"]["user_id"]}')
            trace_data['objects'] = objects
            trace_data['cache_status'] = cache_stats
            trace_data['object_access'] = object_access
            trace_data['timestamps'] = timestamps
            self.replay_trace_queue.put(trace_data)
        except Exception as ex:
             print(ret)
             print(type(ex).__name__)
             print(request)
             self.broken_requests += 1
             #raise NameError('Lets print an error for now')
        
        self.executed_requests += 1
        self.logger.info(f'read_home_timeline: # of requests {self.executed_requests}, # of broken {self.broken_requests}')
        self.concurrent_req.release()
        return
    
    
    
    def run(self):
        threads = []
        print(self.args.exp.trace)
        with open(self.args.exp.trace, 'r') as fd:
            self.logger.info(f'The trace file has been loaded')
            traces = json.load(fd)
            for tr in traces:
                self.concurrent_req.acquire()
                if 'compose_post' in tr:
                    t = threading.Thread(target = self.compose_post , args = (tr['compose_post'],))
                    t.start()
                    threads.append(t)
                elif 'read_home_timeline_pipeline' in tr:
                    t = threading.Thread(target = self.read_home_timeline, args = (tr['read_home_timeline_pipeline'],))
                    t.start()
                    threads.append(t)
                elif 'read_user_timeline_pipeline' in tr:
                    t = threading.Thread(target = self.read_user_timeline , args = (tr['read_user_timeline_pipeline'],))
                    t.start()
                    threads.append(t)

                break
    
            for t in threads:
                t.join()
            self.dump_logs()
    
    
    def dump_logs(self):
        trace_data = [] 
        while not self.replay_trace_queue.empty():
            t = self.replay_trace_queue.get(block=False)
            self.replay_trace_queue.task_done()
            trace_data.append(t)
      
        with open(self.args.exp.logfile, 'w') as fd:
            json.dump(trace_data, fd)
        



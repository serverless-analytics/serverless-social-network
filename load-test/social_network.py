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
                          update_action_limits, zipf)
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
from replay import Replay

# -----------------------------------------------------------------------
# Global variables
# -----------------------------------------------------------------------
logger = None
post_storage_client = None
social_graph_client = None
user_timeline_client = None
home_timeline_client = None
dbs = None
transactions = None

trace = Queue()



'''
def zipf(n_interactions, s, n_users):
    def f(N, k, s):
        return (1/math.pow(k, s))/sigma

    users = list(range(0, n_users))
    random.shuffle(users) # assign rank to users randomly

    weights = [0]*n_users
    sigma = sum([1/math.pow(n, s) for n in range(1, n_users + 1)])
    for i, user in enumerate(users):
        weights[i] = f(N=n_users, k = i + 1, s=s)

    transactions = random.choices(users, weights = weights, k = n_interactions)
    with open('/home/mania/serverless-social-network-multithreaded/functions/results/zipfusers1.2.locust100.512M/users', 'w') as fd:
        fd.write(str(transactions))

    trans = Queue()
    for t in transactions: trans.put(t)
    return trans
'''




def get_mongodb_port_by_container_name(container_name):
    docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    container = docker_client.containers.get(container_name)
    port = int(container.attrs['NetworkSettings']
               ['Ports']['27017/tcp'][0]['HostPort'])
    return port


def init_logger():
    log_file_path = Path(__file__).parent.absolute() / \
        'logs' / (Path(__file__).stem + '.log')
    logger = get_logger(log_file_path=log_file_path,
                        logger_name=Path(__file__).stem)
    logger.info('logger initialization completed')
    return logger


def init_configs():
    global logger
    logger.info('init configs for utils and actions')
    config_path = Path(__file__).parent.absolute() / 'utils' / 'config.ini'
    action_config_path = Path(__file__).parent.absolute() / \
        'actions' / 'common' / 'config.ini'
    init_config(config_path)
    init_config(action_config_path)


def build_runtime_image():
    global logger
    logger.info('build python3action image')
    docker_image_build(build_path=Path(__file__).parent / 'runtimes' / 'social-network-runtime',
                       dockerfile='Dockerfile',
                       tag='mania/social-network-runtime:latest',
                       result=False)
    logger.info('push python3action image')
    docker_image_push(
        tag='mania/social-network-runtime:latest', result=False)


def create_actions_sequences():
    global logger

    logger.info('create actions & sequences')
    actions_dir = Path(__file__).parent.absolute() / 'actions'
    social_network_actions_dir = actions_dir / 'social_network'
    common_dir = actions_dir / 'common'

    for action_path in social_network_actions_dir.iterdir():
        if action_path.is_dir():
            if action_path.stem == 'common':
                continue
            if action_path.stem not in ['compose_post', 'read_social_graph',
                                        'read_post', 'store_post',
                                        'read_user_timeline', 'write_user_timeline',
                                        'read_home_timeline', 'write_home_timeline']:
                continue
            copy_tree(str(common_dir), str(action_path))
            zf_path = action_path / (action_path.stem + '.zip')
            with zipfile.ZipFile(zf_path, 'w') as zf:
                for p in Path(action_path).glob('**/*'):
                    if p.is_file() and p.suffix != '.zip':
                        zf.write(filename=p, arcname=p.relative_to(action_path))
            create_action(action_name=action_path.stem, app_file=zf_path,
                          cpu=1.0, memory=256,
                          docker_image='mania/social-network-runtime')
            zf_path.unlink()
        elif action_path.is_file() and action_path.suffix == '.py':
            create_action(action_name=action_path.stem, app_file=action_path,
                          cpu=1.0, memory=256,
                          docker_image='mania/social-network-runtime')

    create_sequence(sequence_name='write_home_timeline_pipeline',
                    action_list=['read_social_graph', 'write_home_timeline'])

    create_sequence(sequence_name='read_home_timeline_pipeline',
                    action_list=['read_home_timeline', 'read_post'])

    create_sequence(sequence_name='read_user_timeline_pipeline',
                    action_list=['read_user_timeline', 'read_post'])


def init_mongodb(args):
    global logger
    global post_storage_client
    global social_graph_client
    global user_timeline_client
    global home_timeline_client

    logger.info('init mongodb')
    #host_ip_addr = socket.gethostname() + '.ece.cornell.edu'
    #host_ip_addr = socket.gethostname() # Mania: this should be changed to Rodrigo's address 
    #host_ip_addr = "rfonseca-dask.westus2.cloudapp.azure.com"
    host_ip_addr = args.mongo_client

    post_storage_mongodb_ip_addr = host_ip_addr
    post_storage_mongodb_port = get_mongodb_port_by_container_name(
        'post_storage_mongodb')
    post_storage_client = MongoClient(
        post_storage_mongodb_ip_addr, post_storage_mongodb_port)

    social_graph_mongodb_ip_addr = host_ip_addr
    social_graph_mongodb_port = get_mongodb_port_by_container_name(
        'social_graph_mongodb')
    social_graph_client = MongoClient(
        social_graph_mongodb_ip_addr, social_graph_mongodb_port)

    user_timeline_mongodb_ip_addr = host_ip_addr
    user_timeline_mongodb_port = get_mongodb_port_by_container_name(
        'user_timeline_mongodb')
    user_timeline_client = MongoClient(
        user_timeline_mongodb_ip_addr, user_timeline_mongodb_port)

    home_timeline_mongodb_ip_addr = host_ip_addr
    home_timeline_mongodb_port = get_mongodb_port_by_container_name(
        'home_timeline_mongodb')
    home_timeline_client = MongoClient(
        home_timeline_mongodb_ip_addr, home_timeline_mongodb_port)

    '''
    if drop_all_dbs:
        post_storage_client.drop_database('post')
        post_storage_client.drop_database('media')
        if not except_social_graph:
            social_graph_client.drop_database('social_graph')
        social_graph_client.drop_database('user')
        user_timeline_client.drop_database('user_timeline')
        home_timeline_client.drop_database('home_timeline')
    '''
    dbs = {
        'post_storage_mongodb': {
            'ip_addr': post_storage_mongodb_ip_addr,
            'port': post_storage_mongodb_port
        },
        'social_graph_mongodb': {
            'ip_addr': social_graph_mongodb_ip_addr,
            'port': social_graph_mongodb_port
        },
        'user_timeline_mongodb': {
            'ip_addr': user_timeline_mongodb_ip_addr,
            'port': user_timeline_mongodb_port
        },
        'home_timeline_mongodb': {
            'ip_addr': home_timeline_mongodb_ip_addr,
            'port': home_timeline_mongodb_port
        },
    }
    return dbs


def init_social_graph(social_graph_path):
    global logger
    global social_graph_client
    global users

    logger.info('init social graph')
    user_db = social_graph_client['user']
    user_collection = user_db['user']
    user_collection.create_index(
        [('user_id', pymongo.ASCENDING)], name='user_id', unique=True)

    social_graph_db = social_graph_client['social_graph']
    social_graph_collection = social_graph_db['social_graph']
    social_graph_collection.create_index([('user_id', pymongo.ASCENDING)],
                                         name='user_id', unique=True)

    def get_nodes(file):
        global logger
        line = file.readline()
        word = line.split()[0]
        #logger.info(f'*********************** {line}, {word}')
        return int(word)

    def get_edges(file):
        edges = []
        lines = file.readlines()
        for line in lines:
            edges.append(line.split())
        return edges

    def register(user_id=None):
        if user_id is None:
            user_id = random.getrandbits(64)
        first_name = 'first_name_' + str(user_id),
        last_name = 'last_name_' + str(user_id),
        username = 'username_' + str(user_id),
        password = 'password_' + str(user_id),
        user_id = user_id
        document = {
            'first_name': first_name,
            'last_name': last_name,
            'username': username,
            'password': password,
            'user_id': user_id
        }
        user_collection.insert_one(document)

    def follow(user_id, followee_id):
        social_graph_collection.find_one_and_update(filter={'user_id': user_id},
                                                    update={
                                                        '$push': {'followees': followee_id}},
                                                    upsert=True)

    nodes = None
    edges = None
    with open(social_graph_path, 'r') as file:
        nodes = get_nodes(file)
        edges = get_edges(file)

    logger.info(f'upload user nodes {nodes}')
    for i in tqdm(range(1, nodes + 1)):
        register(user_id=i)

    logger.info('upload user edges')
    for edge in tqdm(edges):
        follow(user_id=edge[0], followee_id=edge[1])
        follow(user_id=edge[1], followee_id=edge[0])
    logger.info('finish uploading social graph')

def init_social_graph_manually():
    user_db = social_graph_client['user']
    user_collection = user_db['user']
    user_collection.create_index(
        [('user_id', pymongo.ASCENDING)], name='user_id', unique=True)

    social_graph_db = social_graph_client['social_graph']
    social_graph_collection = social_graph_db['social_graph']
    social_graph_collection.create_index([('user_id', pymongo.ASCENDING)],
                                         name='user_id', unique=True)

    def register(first_name, last_name, username, password, user_id=None):
        if user_id is None:
            user_id = random.getrandbits(64)
        document = {
            'first_name': first_name,
            'last_name': last_name,
            'username': username,
            'password': password,
            'user_id': user_id
        }
        user_collection.insert_one(document)

    def follow(user_id, followee_id):
        social_graph_collection.find_one_and_update(filter={'user_id': user_id},
                                                    update={
                                                        '$push': {'followees': followee_id}},
                                                    upsert=True)

    for user_id in range(1, 10):
        register(first_name='first_name_' + str(user_id),
                 last_name='last_name_' + str(user_id),
                 username='username_' + str(user_id),
                 password='password_' + str(user_id),
                 user_id=user_id)
    follow(4, 1)
    follow(5, 1)
    return



def load_image_sizes():
    sizes = []
    #image_size_path = Path(__file__).parent.absolute() / 'datasets' / 'traces' / 'facebook.2.image.sizes'
    #with open(image_size_path, 'r') as fd:
    #    data = [int(s) for s in fd.read().split('\n')[:-1]]
    #    sizes.extend(data)
    
    image_size_path = Path(__file__).parent.absolute() / 'datasets' / 'socnet_imagesize' / 'instagram.image.sizes.clean'
    with open(image_size_path, 'r') as fd:
        data = [int(s) for s in fd.read().split('\n')[:-1]]
        sizes.extend(data)
    return sizes








class SocialNetworkUser(HttpUser):
    global dbs
    global transactions
    global trace
    global logger
    host = APIHOST
    wait_time = between(1, 2.5)
    n_users = 63392
    medias = set()
    sizes = []
    mutex = threading.Lock()
    index_trans = 0


    def on_start(self):
        self.sizes = load_image_sizes()



    #def on_stop(self):
        #logger.info("Let see how many times thhis function is called")
        #with open('trace.json', 'w') as fd:
        #    json.dump(self.trace, fd)


    @task(0)
    def compose_post(self, _id=None):
        user_id = random.randint(1, self.n_users) if not _id else _id 
        username = 'username_' + str(user_id)
        text = ''.join(random.choices(
            string.ascii_letters + string.digits, k=random.randint(64, 1024)))
        num_user_mentions = random.randint(0, 3)
        user_mention_ids = list()
        for _ in range(num_user_mentions):
            while True:
                user_mention_id = random.randint(1, self.n_users)
                if user_mention_id != user_id and user_mention_id not in user_mention_ids:
                    user_mention_ids.append(user_mention_id)
                    break

        for user_mention_id in user_mention_ids:
            text = text + ' @username_' + str(user_mention_id)
        num_medias = random.randint(0, 5)
        media_ids = list()
        media_types = list()
        media_sizes = list()
        media_contents = list()
        for _ in range(num_medias):
            while True:
                # check if the media id is ever repeated 
                media_id = random.randint(1, sys.maxsize)
                if media_id not in self.medias:
                    self.medias.add(media_id)
                    break

            m_size = random.choice(self.sizes)
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
                'dbs': dbs
            }
        }

        url_params = {'blocking': 'true', 'result': 'true'}
        result = self.client.post(url='/api/' + action_name,
                         params=url_params,
                         json=action_params,
                         auth=(USER_PASS[0], USER_PASS[1]),
                         verify=False,
                         name=action_name)
        logger.info(f'Compose post result is {result.text}')

        try:
            res = result.json()
        except: 
            res = ast.literal_eval(result.text)

        #with self.mutex:
        self.trace.put({
            'compose_post': {
                'username': username,
                'user_id': user_id,
                'text_size': len(text),
                'media_ids': media_ids,
                'media_types': media_types,
                'media_sizes': media_sizes,
                'post_type': 'POST',
                'timestamp': res['post_timestamp'],
                'user_mention_ids': user_mention_ids,
                'post_id': res['post_id']}})
        return



    @task(5)
    def read_home_timeline(self):
        action_name = 'read_home_timeline_pipeline'
        request_id = random.randint(0, sys.maxsize)
        start = random.randint(0, 12)
        stop = start + 10
        user_id = transactions.get()
        transactions.task_done()

        trace_data = {'read_home_timeline_pipeline': {
                        'request_id': request_id,
                        'user_id': user_id,
                        'start': start,
                        'stop': stop}}
        
        action_params = {
            'request_id': request_id,
            'read_home_timeline': {
                'user_id': user_id,
                'start': start,
                'stop': stop,
                'dbs': dbs
            }
        }
        url_params = {'blocking': 'true', 'result': 'true'}
        resp = self.client.post(url='/api/' + action_name,
                         params=url_params,
                         json=action_params,
                         auth=(USER_PASS[0], USER_PASS[1]),
                         verify=False,
                         name=action_name)

        
        try:
            result = json.loads(resp.text)
        except:
            result = ast.literal_eval(resp.text)

        #[2021-09-30 08:13:08,121] rfonseca-dask/CRITICAL/playground:  543497539150576606, dict_keys(['media_id', 'media_type', 'media_size', 'media_content', 'post_id', 'author', 'timestamp', 'post_type'])
        #[2021-09-30 08:13:08,121] rfonseca-dask/CRITICAL/playground: 0 dict_keys(['post_id', 'author', 'text', 'media_ids', 'medias', 'timestamp', 'post_type'])
 
        objects = []
        cache_stats = []
        object_access = []
        logger.info(f'request id is {result["request_id"]}')
        assert(request_id == result["request_id"])
        timestamps = result['timestamps']
        post_ids = result['post_ids']
        for ps in result['posts']:
            cache_stats.append(ps['cache_status'])
            object_access.append(ps['object_access'])
            logger.info(ps.keys())
            logger.info(ps['cache_status'])
            logger.info(ps['object_access'])

        posts = [post  for p in result.get('posts', []) for post in p['posts']]
        for i, post in enumerate(posts):
            #logger.critical(f'oid: {post["post_id"]}, type: text, size: {len(post["text"])}. post: {post.keys()}')
            objects.append({'oid': post['post_id'], 'type': 'text', 'size': len(post['text']), 'author': post['author']['user_id']})
            medias = post['medias']
            for media in medias:
                objects.append({'oid': media['media_id'], 'type': media['media_type'], 'size': media['media_size'], 'author': media['author']['user_id']})
                #logger.critical(f'oid: {media["media_id"]}, type: {media["media_type"]}, size: {media["media_size"]}, author: {media["author"]["user_id"]}')
        trace_data['objects'] = objects
        trace_data['cache_status'] = cache_stats
        trace_data['object_access'] = object_access
        trace_data['timestamps'] = timestamps
        #trace_data['read_home_timeline_pipeline']['objects'] = objects
        trace.put(trace_data)
        return



    @task(5)
    def read_user_timeline(self):
        request_id = random.randint(0, sys.maxsize)
        action_name = 'read_user_timeline_pipeline'
        start = random.randint(0, 12)
        stop = start + 10
        user_id = transactions.get()
        transactions.task_done()
        trace_data = {action_name: {
            'request_id': request_id,               
            'user_id': user_id,
            'start': start, 'stop': stop}}
        

        action_params = {
            'request_id': request_id,
            'read_user_timeline': {
                'user_id': user_id,
                'start': start,
                'stop': stop,
                'dbs': dbs
            }
        }
        url_params = {'blocking': 'true', 'result': 'true'}
        resp = self.client.post(url='/api/' + action_name,
                         params=url_params,
                         json=action_params,
                         auth=(USER_PASS[0], USER_PASS[1]),
                         verify=False,
                         name=action_name)
        try:
            result = json.loads(resp.text)
        except:
            result = ast.literal_eval(resp.text)

 
        objects = []
        cache_stats = []
        object_access = []
        post_ids = result['post_ids']
        logger.info(f'request id is {result["request_id"]}')
        assert(request_id == result["request_id"])
        timestamps = result['timestamps']
        for ps in result['posts']:
            cache_stats.append(ps['cache_status'])
            object_access.append(ps['object_access'])
            logger.info(ps.keys())
            logger.info(ps['cache_status'])
            logger.info(ps['object_access'])
        posts = [post  for p in result.get('posts', []) for post in p['posts']]
        for i, post in enumerate(posts):
            #logger.critical(f'oid: {post["post_id"]}, type: text, size: {len(post["text"])}, post: {post.keys()}')
            objects.append({'oid': post['post_id'], 'type': 'text', 'size': len(post['text']), 'author': post['author']['user_id']})
            medias = post['medias']
            for media in medias:
                objects.append({'oid': media['media_id'], 'type': media['media_type'], 'size': media['media_size'], 'author': media['author']['user_id']})
                #logger.critical(f'oid: {media["media_id"]}, type: {media["media_type"]}, size: {media["media_size"]}, author: {media["author"]["user_id"]}')
        trace_data['objects'] = objects
        trace_data['cache_status'] = cache_stats
        trace_data['object_access'] = object_access
        trace_data['timestamps'] = timestamps
        trace.put(trace_data)
        return



_medias = set()
concurrent_req = threading.Semaphore(50)        

def replay_compose_post(request):
    global _medias
    global dbs
    global concurrent_req

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
            if media_id not in _medias:
                _medias.add(media_id)
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
            'dbs': dbs
        }
    }
    res = invoke_action(action_name='compose_post',
            params=action_params, blocking=True, result=True)
    concurrent_req.release()
    pass


req = 0

def replay_read_home_timeline(request):
    global dbs
    global req
    global concurrent_req
    global logger

    logger.info('prepare replay_read_home_timeline')

    user_id = request['user_id']
    start = request['start']
    stop = request['stop']
    action_params = {
        'read_home_timeline': {
            'user_id': user_id,
            'start': start,
            'stop': stop,
            'dbs': dbs
        }
    }
    result = invoke_action(action_name='read_home_timeline_pipeline',
            params=action_params, blocking=True, result=True)

    if isinstance(result, str):
        print(f'read_home_timeline: req id {req}')
        req += 1
        concurrent_req.release()
        return
        
    #try:
    #    result = json.loads(resp.text)
    #except:
    #    result = ast.literal_eval(resp.text)

    #[2021-09-30 08:13:08,121] rfonseca-dask/CRITICAL/playground:  543497539150576606, dict_keys(['media_id', 'media_type', 'media_size', 'media_content', 'post_id', 'author', 'timestamp', 'post_type'])
    #[2021-09-30 08:13:08,121] rfonseca-dask/CRITICAL/playground: 0 dict_keys(['post_id', 'author', 'text', 'media_ids', 'medias', 'timestamp', 'post_type'])

    trace_data = request

    objects = []
    cache_stats = []
    object_access = []
    logger.info(f'request id is {result["request_id"]}')
    timestamps = result['timestamps']
    post_ids = result['post_ids']
    for ps in result['posts']:
        cache_stats.append(ps['cache_status'])
        object_access.append(ps['object_access'])
        logger.info(ps.keys())
        logger.info(ps['cache_status'])
        logger.info(ps['object_access'])

    posts = [post  for p in result.get('posts', []) for post in p['posts']]
    for i, post in enumerate(posts):
        #logger.critical(f'oid: {post["post_id"]}, type: text, size: {len(post["text"])}. post: {post.keys()}')
        objects.append({'oid': post['post_id'], 'type': 'text', 'size': len(post['text']), 'author': post['author']['user_id']})
        medias = post['medias']
        for media in medias:
            objects.append({'oid': media['media_id'], 'type': media['media_type'], 'size': media['media_size'], 'author': media['author']['user_id']})
            #logger.critical(f'oid: {media["media_id"]}, type: {media["media_type"]}, size: {media["media_size"]}, author: {media["author"]["user_id"]}')
    trace_data['objects'] = objects
    trace_data['cache_status'] = cache_stats
    trace_data['object_access'] = object_access
    trace_data['timestamps'] = timestamps
    replay_trace_queue.put(trace_data)
    print(f'read_home_timeline: req id {req}')
    req += 1
    concurrent_req.release()
    return

def replay_read_user_timeline(request):
    global dbs
    global req
    global concurrent_req
    global logger

    logger.info('prepare replay_read_user_timeline')
    user_id = request['user_id']
    start = request['start']
    stop = request['stop']
    action_params = {
        'read_user_timeline': {
            'user_id': user_id,
            'start': start,
            'stop': stop,
            'dbs': dbs
        }
    }
    result = invoke_action(action_name='read_user_timeline_pipeline',
            params=action_params, blocking=True, result=True)
    
    if isinstance(result, str):
        print(f'read_home_timeline: req id {req}')
        req += 1
        concurrent_req.release()
        return
    #try:
    #    result = json.loads(resp.text)
    #except:
    #    result = ast.literal_eval(resp.text)

    #[2021-09-30 08:13:08,121] rfonseca-dask/CRITICAL/playground:  543497539150576606, dict_keys(['media_id', 'media_type', 'media_size', 'media_content', 'post_id', 'author', 'timestamp', 'post_type'])
    #[2021-09-30 08:13:08,121] rfonseca-dask/CRITICAL/playground: 0 dict_keys(['post_id', 'author', 'text', 'media_ids', 'medias', 'timestamp', 'post_type'])
 
    trace_data = request
    
    objects = []
    cache_stats = []
    object_access = []
    logger.info(f'request id is {result["request_id"]}')
    timestamps = result['timestamps']
    post_ids = result['post_ids']
    for ps in result['posts']:
        cache_stats.append(ps['cache_status'])
        object_access.append(ps['object_access'])
        logger.info(ps.keys())
        logger.info(ps['cache_status'])
        logger.info(ps['object_access'])

    posts = [post  for p in result.get('posts', []) for post in p['posts']]
    for i, post in enumerate(posts):
        #logger.critical(f'oid: {post["post_id"]}, type: text, size: {len(post["text"])}. post: {post.keys()}')
        objects.append({'oid': post['post_id'], 'type': 'text', 'size': len(post['text']), 'author': post['author']['user_id']})
        medias = post['medias']
        for media in medias:
            objects.append({'oid': media['media_id'], 'type': media['media_type'], 'size': media['media_size'], 'author': media['author']['user_id']})
            #logger.critical(f'oid: {media["media_id"]}, type: {media["media_type"]}, size: {media["media_size"]}, author: {media["author"]["user_id"]}')
    trace_data['objects'] = objects
    trace_data['cache_status'] = cache_stats
    trace_data['object_access'] = object_access
    trace_data['timestamps'] = timestamps
    replay_trace_queue.put(trace_data)
    print(f'read_user_timeline: req id {req}')
    req += 1
    concurrent_req.release()
    pass


def get_user_ids(dbs):
    global social_graph_client
    global logger

    logger.info('get list of users')
    user_db = social_graph_client['user']
    user_collection = user_db['user']
    user_collection.create_index(
        [('user_id', pymongo.ASCENDING)], name='user_id', unique=True)
    
    cursor = user_collection.find({})
    logger.error(f'cursor {list(cursor)}, {user_db}')
    for d in cursor:
        logger.error(f'{d}')
    return cursor



def compose_post(_id=None, n_users=100):
    global _medias
    global dbs


    user_id = random.randint(1, n_users) if not _id else _id 
    username = 'username_' + str(user_id)
    text = ''.join(random.choices(
        string.ascii_letters + string.digits, k=random.randint(64, 1024)))
    num_user_mentions = random.randint(0, 3)
    user_mention_ids = list()
    for _ in range(num_user_mentions):
        while True:
            user_mention_id = random.randint(1, n_users)
            if user_mention_id != user_id and user_mention_id not in user_mention_ids:
                user_mention_ids.append(user_mention_id)
                break

    for user_mention_id in user_mention_ids:
        text = text + ' @username_' + str(user_mention_id)
    num_medias = random.randint(0, 5)
    media_ids = list()
    media_types = list()
    media_sizes = list()
    media_contents = list()
    for _ in range(num_medias):
        while True:
            media_id = random.randint(1, sys.maxsize)
            if media_id not in _medias:
                _medias.add(media_id)
                break

        m_size = random.choice(sizes)
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
            'dbs': dbs
        }
    }
    res = invoke_action(action_name='compose_post',
            params=action_params, blocking=True, result=True)
    return res




sizes = load_image_sizes() 

def run(args):
    global logger
    global dbs
    global trace
    global transactions 
    global trace 
    #trace = Queue()
    
    run_locust_test=False
    # -----------------------------------------------------------------------
    # Init logger & configs
    # -----------------------------------------------------------------------
    logger = init_logger()
    init_configs()

    # -----------------------------------------------------------------------
    # Build image
    # -----------------------------------------------------------------------
    build_runtime_image()

    # -----------------------------------------------------------------------
    # Create actions & sequences
    # -----------------------------------------------------------------------
    #create_actions_sequences()

    # -----------------------------------------------------------------------
    # MongoDB
    # -----------------------------------------------------------------------
    dbs = init_mongodb(args)
    print('dbs: {}\n'.format(dbs))


    #-----------------------------------------------------------------------
    # preload database
    #-----------------------------------------------------------------------
    if args.exp_type == 'preload-db':
        if args.exp.load_social_graph == 'file':
            init_social_graph(social_graph_path=args.exp.social_graph)
        elif args.exp.load_social_graph == 'manual':
            init_social_graph_manually()


        if args.exp.mode == 'exhustive':
            n_users = args.exp.n_users
            for i in range(0, n_users):
                for p in range(0, args.exp.post_per_user):
                    logger.info(f'Compose post for user {i}, post {p}')
                    compose_post(i, n_users)
    elif args.exp_type == 'replay': 
        logger.info('replay trace')
        replay = Replay(args=args, dbs = dbs, logger = logger)
        replay.run()
    elif args.exp_type == 'locust':
        logger.info('locust load testing starts')

        n_users = 63392
        n_interactions = 1000000
        s = 1.2
        transactions = zipf(n_interactions = args.exp.n_interactions, s = args.exp.s, n_users=args.exp.n_users)
        
        setup_logging('INFO', None)

        #initialize_user_posts();

        # setup Environment and Runner
        env = Environment(user_classes=[SocialNetworkUser])
        env.create_local_runner()

        # start a WebUI instance
        external_ip = urllib.request.urlopen(
            'https://ident.me').read().decode('utf8')
        #env.create_web_ui(external_ip, 8089)

        # start a greenlet that periodically outputs the current stats
        gevent.spawn(stats_printer(env.stats))

        # start the test
        env.runner.start(user_count=args.exp.parallelism, spawn_rate=5)

        # in 60 seconds stop the runner
        gevent.spawn_later(args.exp.duration, lambda: env.runner.quit())

        # wait for the greenlets
        env.runner.greenlet.join()

        # stop the web server for good measures
        #env.web_ui.stop()

        trace_data = [] 
        while not trace.empty():
            t = trace.get(block=False)
            trace.task_done()
            trace_data.append(t)
        with open(args.exp.logfile, 'w') as fd:
            json.dump(trace_data, fd)

        #csv_base_filepath = Path(
        #    __file__).parent.absolute() / 'locust' / 'openwhisk'
        #write_csv_files(environment=env, base_filepath=str(csv_base_filepath))

        logger.info('locust load testing ended')
    else:
        user_id = 1
        username = 'username_' + str(user_id)
        text = ''.join(random.choices(
            string.ascii_letters + string.digits, k=100))

        user_mention_ids = [2, 3]
        for user_mention_id in user_mention_ids:
            text = text + ' @username_' + str(user_mention_id)
        num_medias = random.randint(0, 5)
        media_ids = list()
        media_types = list()
        for _ in range(num_medias):
            media_ids.append(random.randint(1, sys.maxsize))
            media_types.append('PIC')

        params = {
            'compose_post': {
                'username': username,
                'user_id': user_id,
                'text': text,
                'media_ids': media_ids,
                'media_types': media_types,
                'post_type': 'POST',
                'dbs': dbs
            }
        }
        res = invoke_action(action_name='compose_post',
                            params=params, blocking=True, result=True)
        print('res: {}\n'.format(res))
        print('post: {}\n'.format(list(post_storage_client.post.post.find({}))))
        print('user_timeline: {}\n'.format(
            list(user_timeline_client.user_timeline.user_timeline.find({}))))
        print('home_timeline: {}\n'.format(
            list(home_timeline_client.home_timeline.home_timeline.find({}))))

        params = {
            'read_home_timeline': {
                'user_id': 2,
                'start': 0,
                'stop': 1,
                'dbs': dbs
            }
        }
        res = invoke_action(action_name='read_home_timeline_pipeline',
                            params=params, blocking=True, result=True)
        print('read_home_timeline: {}\n'.format(res['posts']))

        params = {
            'read_user_timeline': {
                'user_id': user_id,
                'start': 0,
                'stop': 1,
                'dbs': dbs
            }
        }
        res = invoke_action(action_name='read_user_timeline_pipeline',
                            params=params, blocking=True, result=True)
        print('read_user_timeline: {}'.format(res['posts']))


if __name__ == "__main__":
    main()

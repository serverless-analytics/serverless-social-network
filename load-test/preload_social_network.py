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

import actions.social_network.standalone as standalone 


# -----------------------------------------------------------------------
# Global variables
# -----------------------------------------------------------------------
logger = None
post_storage_client = None
social_graph_client = None
user_timeline_client = None
home_timeline_client = None
dbs = None


def get_mongodb_port_by_container_name(container_name):
    docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    container = docker_client.containers.get(container_name)
    port = int(container.attrs['NetworkSettings']
               ['Ports']['27017/tcp'][0]['HostPort'])
    return port


def init_logger():
    global logger
    log_file_path = Path(__file__).parent.absolute() / \
        'logs' / (Path(__file__).stem + '.log')
    logger = get_logger(log_file_path=log_file_path,
                        logger_name=Path(__file__).stem)
    logger.info('logger initialization completed')


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


def init_mongodb(drop_all_dbs=True, except_social_graph=True):
    global logger
    global post_storage_client
    global social_graph_client
    global user_timeline_client
    global home_timeline_client

    logger.info('init mongodb')
    #host_ip_addr = socket.gethostname() + '.ece.cornell.edu'
    host_ip_addr = socket.gethostname() # Mania: this should be changed to Rodrigo's address 

    post_storage_mongodb_ip_addr = host_ip_addr
    post_storage_mongodb_port = get_mongodb_port_by_container_name(
        'post_storage_mongodb1')
    post_storage_client = MongoClient(
        post_storage_mongodb_ip_addr, post_storage_mongodb_port)

    social_graph_mongodb_ip_addr = host_ip_addr
    social_graph_mongodb_port = get_mongodb_port_by_container_name(
        'social_graph_mongodb1')
    social_graph_client = MongoClient(
        social_graph_mongodb_ip_addr, social_graph_mongodb_port)

    user_timeline_mongodb_ip_addr = host_ip_addr
    user_timeline_mongodb_port = get_mongodb_port_by_container_name(
        'user_timeline_mongodb1')
    user_timeline_client = MongoClient(
        user_timeline_mongodb_ip_addr, user_timeline_mongodb_port)

    home_timeline_mongodb_ip_addr = host_ip_addr
    home_timeline_mongodb_port = get_mongodb_port_by_container_name(
        'home_timeline_mongodb1')
    home_timeline_client = MongoClient(
        home_timeline_mongodb_ip_addr, home_timeline_mongodb_port)

    if drop_all_dbs:
        post_storage_client.drop_database('post')
        post_storage_client.drop_database('media')
        if not except_social_graph:
            social_graph_client.drop_database('social_graph')
        social_graph_client.drop_database('user')
        user_timeline_client.drop_database('user_timeline')
        home_timeline_client.drop_database('home_timeline')

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



def load_image_sizes():
    sizes = []
    image_size_path = Path(__file__).parent.absolute() / 'datasets' / 'traces' / 'facebook.2.image.sizes'
    with open(image_size_path, 'r') as fd:
        data = [int(s) for s in fd.read().split('\n')[:-1]]
        sizes.extend(data)
    
    image_size_path = Path(__file__).parent.absolute() / 'datasets' / 'traces' / 'instagram.image.sizes.clean'
    with open(image_size_path, 'r') as fd:
        data = [int(s) for s in fd.read().split('\n')[:-1]]
        sizes.extend(data)
    return sizes


_medias = set()


def replay_compose_post(request):
    global _medias
    global dbs
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
    pass


def replay_read_home_timeline(request):
    global dbs
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
    res = invoke_action(action_name='read_home_timeline_pipeline',
            params=action_params, blocking=True, result=True)

    #print(res)
    pass


def replay_read_user_timeline(request):
    global dbs
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
    res = invoke_action(action_name='read_user_timeline_pipeline',
            params=action_params, blocking=True, result=True)
    #print(res)
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
    global logger
    
    #get_user_ids(dbs)
    logger.info('Call compose post')
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
    res = standalone.compose_post(args=action_params)
    return res




sizes = load_image_sizes() 

def main(run_locust_test=True):
    global logger
    global dbs

    # -----------------------------------------------------------------------
    # Init logger & configs
    # -----------------------------------------------------------------------
    init_logger()
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
    drop_all_dbs=True
    except_social_graph=False
    dbs = init_mongodb(drop_all_dbs=drop_all_dbs, except_social_graph=except_social_graph)
    # print('dbs: {}\n'.format(dbs))

    # -----------------------------------------------------------------------
    # Init social graph
    # -----------------------------------------------------------------------
    if run_locust_test:
        if (drop_all_dbs) and (not except_social_graph):
            init_social_graph(social_graph_path=Path(__file__).parent /
                          'datasets' / 'social_graph' / 'socfb-Reed98.mtx')
    
    load_database = True
    replay_trace = False
    if load_database:
        n_users = 962
        for i in range(0, n_users):
            for p in range(0, 20):
                logger.info(f'user {i}, post {p} ')
                compose_post(i, n_users)


if __name__ == "__main__":
    main()

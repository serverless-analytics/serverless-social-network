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
                       tag='zzhou612/social-network-runtime:latest',
                       result=False)
    logger.info('push python3action image')
    docker_image_push(
        tag='zzhou612/social-network-runtime:latest', result=False)


def create_actions_sequences():
    global logger

    logger.info('create actions & sequences')
    actions_dir = Path(__file__).parent.absolute() / 'actions'
    social_network_actions_dir = actions_dir / 'social-network'
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
                          docker_image='zzhou612/social-network-runtime')
            zf_path.unlink()
        elif action_path.is_file() and action_path.suffix == '.py':
            create_action(action_name=action_path.stem, app_file=action_path,
                          cpu=1.0, memory=256,
                          docker_image='zzhou612/social-network-runtime')

    create_sequence(sequence_name='write_home_timeline_pipeline',
                    action_list=['read_social_graph', 'write_home_timeline'])

    create_sequence(sequence_name='read_home_timeline_pipeline',
                    action_list=['read_home_timeline', 'read_post'])

    create_sequence(sequence_name='read_user_timeline_pipeline',
                    action_list=['read_user_timeline', 'read_post'])


def init_mongodb(drop_all_dbs=True, except_social_graph=False):
    global logger
    global post_storage_client
    global social_graph_client
    global user_timeline_client
    global home_timeline_client

    logger.info('init mongodb')
    #host_ip_addr = socket.gethostname() + '.ece.cornell.edu'
    host_ip_addr = socket.gethostname()

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

    if drop_all_dbs:
        post_storage_client.drop_database('post')
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
        line = file.readline()
        word = line.split()[0]
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

    logger.info('upload user nodes')
    for i in tqdm(range(1, nodes + 1)):
        register(user_id=i)

    logger.info('upload user edges')
    for edge in tqdm(edges):
        follow(user_id=edge[0], followee_id=edge[1])
        follow(user_id=edge[1], followee_id=edge[0])
    logger.info('finish uploading social graph')


class SocialNetworkUser(HttpUser):
    global dbs
    host = APIHOST
    wait_time = between(1, 2.5)

    # def wait_time(self):
    #     return np.random.exponential(scale=1)
    def on_start(self):
        for i in range(1, 962):
            self.compose_post(i)



    @task(2)
    def compose_post(self, _id=None):
        user_id = random.randint(1, 962) if not _id else _id 
        username = 'username_' + str(user_id)
        text = ''.join(random.choices(
            string.ascii_letters + string.digits, k=100))
        num_user_mentions = random.randint(0, 3)
        user_mention_ids = list()
        for _ in range(num_user_mentions):
            while True:
                user_mention_id = random.randint(1, 962)
                if user_mention_id != user_id and user_mention_id not in user_mention_ids:
                    user_mention_ids.append(user_mention_id)
                    break
        for user_mention_id in user_mention_ids:
            text = text + ' @username_' + str(user_mention_id)
        num_medias = random.randint(0, 5)
        media_ids = list()
        media_types = list()
        for _ in range(num_medias):
            media_ids.append(random.randint(1, sys.maxsize))
            media_types.append('PIC')

        action_name = 'compose_post'
        action_params = {
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
        url_params = {'blocking': 'true', 'result': 'false'}
        #self.client.post(url='/api/v1/namespaces/' + NAMESPACE + '/actions/' + action_name,
        self.client.post(url='/api/' + action_name,
                         params=url_params,
                         json=action_params,
                         auth=(USER_PASS[0], USER_PASS[1]),
                         verify=False,
                         name=action_name)



    @task(4)
    def read_home_timeline(self):
        action_name = 'read_home_timeline_pipeline'
        user_id = random.randint(1, 962)
        start = random.randint(0, 100)
        stop = start + 10
        action_params = {
            'read_home_timeline': {
                'user_id': user_id,
                'start': start,
                'stop': stop,
                'dbs': dbs
            }
        }
        url_params = {'blocking': 'true', 'result': 'false'}
        #self.client.post(url='/api/v1/namespaces/' + NAMESPACE + '/actions/' + action_name,
        self.client.post(url='/api/' + action_name,
                         params=url_params,
                         json=action_params,
                         auth=(USER_PASS[0], USER_PASS[1]),
                         verify=False,
                         name=action_name)



    @task(4)
    def read_user_timeline(self):
        action_name = 'read_user_timeline_pipeline'
        user_id = random.randint(1, 962)
        start = random.randint(0, 100)
        stop = start + 10
        action_params = {
            'read_user_timeline': {
                'user_id': user_id,
                'start': start,
                'stop': stop,
                'dbs': dbs
            }
        }
        url_params = {'blocking': 'true', 'result': 'false'}
        #self.client.post(url='/api/v1/namespaces/' + NAMESPACE + '/actions/' + action_name,
        self.client.post(url='/api/' + action_name,
                         params=url_params,
                         json=action_params,
                         auth=(USER_PASS[0], USER_PASS[1]),
                         verify=False,
                         name=action_name)


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
    create_actions_sequences()

    # -----------------------------------------------------------------------
    # MongoDB
    # -----------------------------------------------------------------------
    dbs = init_mongodb(drop_all_dbs=True)
    # print('dbs: {}\n'.format(dbs))

    # -----------------------------------------------------------------------
    # Init social graph
    # -----------------------------------------------------------------------
    if run_locust_test:
        init_social_graph(social_graph_path=Path(__file__).parent /
                          'datasets' / 'social_graph' / 'socfb-Reed98.mtx')
    else:
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

    # -----------------------------------------------------------------------
    # Invoke actions
    # -----------------------------------------------------------------------
    if run_locust_test:
        logger.info('locust load testing starts')

        setup_logging('INFO', None)

        #initialize_user_posts();

        # setup Environment and Runner
        env = Environment(user_classes=[SocialNetworkUser])
        env.create_local_runner()

        # start a WebUI instance
        external_ip = urllib.request.urlopen(
            'https://ident.me').read().decode('utf8')
        env.create_web_ui(external_ip, 8089)

        # start a greenlet that periodically outputs the current stats
        gevent.spawn(stats_printer(env.stats))

        # start the test
        env.runner.start(user_count=1, spawn_rate=5)

        # in 60 seconds stop the runner
        gevent.spawn_later(240, lambda: env.runner.quit())

        # wait for the greenlets
        env.runner.greenlet.join()

        # stop the web server for good measures
        env.web_ui.stop()

        csv_base_filepath = Path(
            __file__).parent.absolute() / 'locust' / 'openwhisk'
        write_csv_files(environment=env, base_filepath=str(csv_base_filepath))

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

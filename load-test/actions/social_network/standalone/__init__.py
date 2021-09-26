import random
import re
from threading import Thread
import logging

import aiohttp
import azure.functions as func
from requests.models import Response

import pymongo
from pymongo import MongoClient
import logging

from actions.common.lru import LruCache
from actions.common.utils import get_timestamp_ms


CACHE_SIZE=128*1024*1024

sp_mongo_client = None
md_mongo_client = None
sg_mongo_client = None
ht_mongo_client = None
ut_mongo_client = None
lru_cache = None

def compose_post(args):
    # -----------------------------------------------------------------------
    # Parse params
    # -----------------------------------------------------------------------
    timestamps = {
        'main_start_ms': 0,
        'main_end_ms': 0,
        'minio_get_ms': 0,
        'minio_put_ms': 0
    }

    timestamps['main_start_ms'] = get_timestamp_ms()
    
    params = args.get('compose_post', args)
    username = params['username']
    user_id = params['user_id']
    text = params['text']
    media_ids = params['media_ids']
    media_types = params['media_types']
    media_size = params['media_sizes']
    media_content = params['media_contents']
    post_type = params['post_type']
    dbs = params['dbs']

    logging.critical(f'compose_post with args: {username}, {user_id}, {media_ids}, {media_size}')
    # -----------------------------------------------------------------------
    # Action execution
    # -----------------------------------------------------------------------
    # construct post
    post_timestamp = get_timestamp_ms()
    post_id = random.getrandbits(63)
    author = {
        'user_id': user_id,
        'username': username
    }
    medias = list()
    for i in range(len(media_ids)):
        medias.append({
            'media_id': media_ids[i],
            'media_type': media_types[i],
            'media_size': media_size[i],
            #'media_content': media_content[i]
        })

    post = {
        'post_id': post_id,
        'author': author,
        'text': text,
        'media_ids': media_ids,
        'medias': medias,
        'timestamp': post_timestamp,
        'post_type': post_type
    }


    # parse user mentions
    user_mention_names = [username[1:]
                          for username in re.findall('@[a-zA-Z0-9-_]+', text)]

    response = store_post(
            args = {
                'store_post': {
                    'post': post,
                    'dbs': dbs
                }
            })

    
    for i in range(len(media_ids)):
        media = {
                'media_id': media_ids[i],
                'media_type': media_types[i],
                'media_size': media_size[i],
                'media_content': media_content[i],
                'post_id': post_id,
                'author': author,
                'timestamp': post_timestamp,
                'post_type': post_type}
        
        response = store_media(
                args = {
                    'store_media': {
                        'media': media,
                        'dbs': dbs
                        }
                    })
    
    logging.critical(f'composet_post: user = {user_id}, post = {post}')

    response = write_user_timeline(
            args= {
                'write_user_timeline': {
                    'user_id': user_id,
                    'post_id': post_id,
                    'timestamp': post_timestamp,
                    'dbs': dbs
                    }
                })


    response = write_home_timeline_pipeline(
        args= {
            'read_social_graph': {
                'user_id': user_id,
                'post_id': post_id,
                'post_timestamp': post_timestamp,
                'user_mention_names': user_mention_names,
                'dbs': dbs
            }
        })



    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result = dict()
    result['timestamps'] = timestamps
    result['post_id'] = post_id
    result['user_mention_names'] = user_mention_names
    result['post_timestamp'] = post_timestamp
    return result




def store_post(args):
    global sp_mongo_client
    

    # -----------------------------------------------------------------------
    # Parse params
    # -----------------------------------------------------------------------
    timestamps = {
        'main_start_ms': 0,
        'main_end_ms': 0,
        'minio_get_ms': 0,
        'minio_put_ms': 0
    }
    
    timestamps['main_start_ms'] = get_timestamp_ms()
    params = args.get('store_post', args)
    post = params['post']
    dbs = params['dbs']

    # -----------------------------------------------------------------------
    # Action execution
    # -----------------------------------------------------------------------
    mongodb_ip_addr = dbs['post_storage_mongodb']['ip_addr']
    mongodb_port = dbs['post_storage_mongodb']['port']
    if sp_mongo_client is None:
        sp_mongo_client = MongoClient(mongodb_ip_addr, mongodb_port)

    post_db = sp_mongo_client['post']
    post_collection = post_db['post']
    post_collection.create_index([('post_id', pymongo.ASCENDING)],
                                 name='post_id', unique=True)
    post_collection.insert_one(post)

    #result = post_collection.find_one()
    #logging.error(f'store_post: find_one result is {result}')

    #logging.critical("After store post read the post_db")
    #for x in post_collection.find():
    #    logging.critical(f'{x}')
    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result = dict()
    result['timestamps'] = timestamps
    logging.error(f'store_post: post is {post}, result is {result}')
    return result
    pass


def store_media(args):
    global md_mongo_client
    
    # -----------------------------------------------------------------------
    # Parse params
    # -----------------------------------------------------------------------
    timestamps = {
        'main_start_ms': 0,
        'main_end_ms': 0,
        'minio_get_ms': 0,
        'minio_put_ms': 0
    }
    
    timestamps['main_start_ms'] = get_timestamp_ms()
    params = args.get('store_media', args)
    media = params['media']
    dbs = params['dbs']

    # -----------------------------------------------------------------------
    # Action execution
    # -----------------------------------------------------------------------
    mongodb_ip_addr = dbs['post_storage_mongodb']['ip_addr']
    mongodb_port = dbs['post_storage_mongodb']['port']
    if md_mongo_client is None:
        md_mongo_client = MongoClient(mongodb_ip_addr, mongodb_port)

    post_db = md_mongo_client['media']
    post_collection = post_db['media']
    post_collection.create_index([('media_id', pymongo.ASCENDING)],
                                 name='media_id', unique=True)
    post_collection.insert_one(media)

    #result = post_collection.find_one()
    #logging.error(f'store_post: find_one result is {result}')

    #logging.critical("After store post read the post_db")
    #for x in post_collection.find():
    #    logging.critical(f'{x}')
    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result = dict()
    result['timestamps'] = timestamps
    logging.info(f'store_media: with id {media["media_id"]}, size: {media["media_size"]}')
    return result



def write_home_timeline(args):
    global ht_mongo_client
    
    timestamps = {
        'main_start_ms': 0,
        'main_end_ms': 0,
        'minio_get_ms': 0,
        'minio_put_ms': 0
    }
    timestamps['main_start_ms'] = get_timestamp_ms()
    #print(f'write_home_timeline {args}')
    params = args.get('write_home_timeline', args)
    user_id = params['user_id']
    post_id = params['post_id']
    post_timestamp = params['post_timestamp']
    home_timeline_ids = params['home_timeline_ids']
    dbs = params['dbs']

    # -----------------------------------------------------------------------
    # Action execution
    # -----------------------------------------------------------------------
    mongodb_ip_addr = dbs['home_timeline_mongodb']['ip_addr']
    mongodb_port = dbs['home_timeline_mongodb']['port']
    if ht_mongo_client is None:
       ht_mongo_client = MongoClient(mongodb_ip_addr, mongodb_port)

    home_timeline_db = ht_mongo_client['home_timeline']
    home_timeline_collection = home_timeline_db['home_timeline']
    home_timeline_collection.create_index([('user_id', pymongo.ASCENDING)],
                                          name='user_id', unique=True)
    for home_timeline_id in home_timeline_ids:
        home_timeline_collection.find_one_and_update(filter={'user_id': home_timeline_id},
                                                     update={'$push': {
                                                         'posts': {
                                                             'post_id': post_id,
                                                             'timestamp': post_timestamp
                                                         }
                                                     }},
                                                     upsert=True)

    

    #logging.critical("After store post read the post_db")
    #for x in home_timeline_collection.find():
    #    logging.critical(f'\t inside home_timeline collection {x}')


    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result = dict()
    result['timestamps'] = timestamps
    logging.error(f'write_home_timeline: user_id = {user_id}, post_id = {post_id}, result is {result}')
    pass

def write_user_timeline(args):
    global ut_mongo_client
    # -----------------------------------------------------------------------
    # Parse params
    # -----------------------------------------------------------------------
    timestamps = {
        'main_start_ms': 0,
        'main_end_ms': 0,
        'minio_get_ms': 0,
        'minio_put_ms': 0
    }
    timestamps['main_start_ms'] = get_timestamp_ms()
    params = args.get('write_user_timeline', args)
    user_id = params['user_id']
    post_id = params['post_id']
    timestamp = params['timestamp']
    dbs = params['dbs']

    # -----------------------------------------------------------------------
    # Action execution
    # -----------------------------------------------------------------------
    mongodb_ip_addr = dbs['user_timeline_mongodb']['ip_addr']
    mongodb_port = dbs['user_timeline_mongodb']['port']
    if ut_mongo_client is None:
        ut_mongo_client = MongoClient(mongodb_ip_addr, mongodb_port)

    user_timeline_db = ut_mongo_client['user_timeline']
    user_timeline_collection = user_timeline_db['user_timeline']
    user_timeline_collection.create_index([('user_id', pymongo.ASCENDING)],
                                          name='user_id', unique=True)

    user_timeline_collection.find_one_and_update(filter={'user_id': user_id},
                                                 update={'$push': {
                                                     'posts': {
                                                         'post_id': post_id,
                                                         'timestamp': timestamp
                                                     }
                                                 }},
                                                 upsert=True)

    result = user_timeline_collection.find_one()

    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result = dict()
    result['timestamps'] = timestamps
    logging.error(f'write_user_timeline: user_id = {user_id}, post_id = {post_id}, result is {result}')
    return result


def read_social_graph(args, locality = None):
    global sg_mongo_client
    global lru_cache
    # -----------------------------------------------------------------------
    # Parse params
    # -----------------------------------------------------------------------
    timestamps = {
        'main_start_ms': 0,
        'main_end_ms': 0,
        'minio_get_ms': 0,
        'minio_put_ms': 0
    }
    timestamps['main_start_ms'] = get_timestamp_ms()
    #print(args)
    params = args.get('read_social_graph', args)
    user_id = params['user_id']
    post_id = params['post_id']
    post_timestamp = params['post_timestamp']
    user_mention_names = params['user_mention_names']
    dbs = params['dbs']

    # -----------------------------------------------------------------------
    # Action execution
    # -----------------------------------------------------------------------
    if not lru_cache:
        lru_cache = LruCache(capacity = CACHE_SIZE, name=f'social_graph')

        
    mongodb_ip_addr = dbs['social_graph_mongodb']['ip_addr']
    mongodb_port = dbs['social_graph_mongodb']['port']
    if sg_mongo_client is None:
        sg_mongo_client = MongoClient(mongodb_ip_addr, mongodb_port)

    user_db = sg_mongo_client['user']
    user_collection = user_db['user']
    social_graph_db = sg_mongo_client['social_graph']
    social_graph_collection = social_graph_db['social_graph']

    home_timeline_ids = list()
    
    
    # mentioned users
    for user_mention_name in user_mention_names:
        doc = user_collection.find_one(filter={'username': user_mention_name})
        if doc is None:
            raise Exception(
                '{} not found in user collection'.format(user_mention_name))
        user_mention_id = doc['user_id']
        home_timeline_ids.append(user_mention_id)

    # followers
    cursor = lru_cache.get(user_id)
    if cursor == -1: 
        cursor = social_graph_collection.find(filter={'followees': user_id})
        lru_cache.put(user_id, cursor)

    for doc in cursor:
        follower_id = doc['user_id']
        home_timeline_ids.append(follower_id)

    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result = dict()
    result['timestamps'] = timestamps
    result['write_home_timeline'] = {
        'user_id': user_id,
        'post_id': post_id,
        'post_timestamp': post_timestamp,
        'home_timeline_ids': home_timeline_ids,
        'dbs': dbs
    }
    logging.error(f'read_social_graph: user_id={user_id}, post_id={post_id}, result is {result}')
    return result




def write_home_timeline_pipeline(args):
    timestamps = {
        'main_start_ms': 0,
        'main_end_ms': 0,
        'minio_get_ms': 0,
        'minio_put_ms': 0
    }

    timestamps['main_start_ms'] = get_timestamp_ms()
    
    params  = args.get('read_social_graph', args)
    user_id = params['user_id']
    post_id = params['post_id']
    post_timestamp = params['post_timestamp']
    user_mention_names = params['user_mention_names']
    dbs = params['dbs']

    # -----------------------------------------------------------------------
    # Action execution
    # -----------------------------------------------------------------------
    # construct post
    
    response = read_social_graph(
        args = {
            'read_social_graph': {
                'user_id': user_id,
                'post_id': post_id,
                'post_timestamp': post_timestamp,
                'user_mention_names': user_mention_names,
                'dbs': dbs,
                'locality': user_id
            }
        },
        locality = user_id)


    home_timeline_ids = response['write_home_timeline']['home_timeline_ids']
    logging.warning(f'read_social_graph response is {response}')
    response_write = write_home_timeline(
        args= response)


    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result = dict()
    result['timestamps'] = timestamps
    result['post_id'] = post_id
    result['user_mention_names'] = user_mention_names
    return result

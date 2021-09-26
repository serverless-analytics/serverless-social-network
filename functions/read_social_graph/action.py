import logging
from pymongo import MongoClient
from common.utils import get_timestamp_ms, invoke_action
from common.lru import LruCache
from common.config import CACHE_SIZE 


lru_cache = None
mongo_client = None


def execute(args, worker=None):
    global mongo_client
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
        lru_cache = LruCache(capacity = CACHE_SIZE, name=f'{worker.replace(":", "_")}.social_graph')

    
    mongodb_ip_addr = dbs['social_graph_mongodb']['ip_addr']
    mongodb_port = dbs['social_graph_mongodb']['port']
    if mongo_client is None:
        mongo_client = MongoClient(mongodb_ip_addr, mongodb_port)

    user_db = mongo_client['user']
    user_collection = user_db['user']
    social_graph_db = mongo_client['social_graph']
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

import logging
from pymongo import MongoClient
from common.utils import get_timestamp_ms
from common.lru import LruCache
from common.config import CACHE_SIZE 


mongo_client = None
lru_cache = None


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
    params = args #.get('read_post', args)
    post_ids = params['post_ids']
    dbs = params['dbs']

    # -----------------------------------------------------------------------
    # Action execution
    # -----------------------------------------------------------------------
    if not lru_cache:
        lru_cache = LruCache(capacity = CACHE_SIZE, name=f'{worker.replace(":", "_")}.post_media_cache')


    mongodb_ip_addr = dbs['post_storage_mongodb']['ip_addr']
    mongodb_port = dbs['post_storage_mongodb']['port']
    if mongo_client is None:
        mongo_client = MongoClient(mongodb_ip_addr, mongodb_port)

    post_db = mongo_client['post']
    post_collection = post_db['post']

    media_db = mongo_client['media']
    media_collection = media_db['media']


    posts = list()

    for post_id in post_ids:
        post = lru_cache.get(post_id)
        if post == -1: 
            post = post_collection.find_one(filter={'post_id': post_id})
            lru_cache.put(post_id, post)

        medias = list()
        for media_id in post['media_ids']:
            media = lru_cache.get(media_id)
            if media == -1:
                media = media_collection.find_one(filter={'media_id': media_id})
                lru_cache.put(media_id, media)
            media.pop('_id', None)
            medias.append(media)
        post['medias'] = medias


        post.pop('_id', None)  # '_id': ObjectId('5fa8ade6949bf3bd67ed5aaf')
        posts.append(post)

    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result = dict()
    result['timestamps'] = timestamps
    result['posts'] = posts
    return result

import logging
from pymongo import MongoClient
from common.utils import get_timestamp_ms
from common.lru import LruCache
from common.config import CACHE_SIZE 
from pympler import asizeof


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
    result = dict()
    
    result['posts'] = []
    result['object_access'] = []
    result['cache_status'] = {}
    try:
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
        object_access = []
        for post_id in post_ids:
            miss = 'n'
            evicted = []
            post = lru_cache.get(post_id)
            if post == -1: 
                miss = 'y'
                post = post_collection.find_one(filter={'post_id': post_id})
                post.pop('_id', None)  # '_id': ObjectId('5fa8ade6949bf3bd67ed5aaf')
                evicted = lru_cache.put(key = post_id, value = post, size = asizeof.asizeof(post))
            else:
                post = post.get('value', None)
            object_access.append({'oid': post_id , 'miss': miss, 
                'evicted': evicted, 'name': lru_cache.name})
    
            medias = list()
            for media_id in post['media_ids']:
                miss = 'n'
                evicted = []
                media = lru_cache.get(media_id)
                if media == -1:
                    miss = 'y'
                    media = media_collection.find_one(filter={'media_id': media_id})
                    media.pop('_id', None)
                    evicted = lru_cache.put(media_id, value = media['media_content'], size= media['media_size'])
                else:
                    media['media_content'] = media['value']
                    media['media_size'] = media['size']
                object_access.append({'oid': media_id , 'miss': miss, 
                    'evicted': evicted, 'name': lru_cache.name})
                medias.append({'id': media_id, 'content': media['media_content'], 'size': media['media_size']})
                #medias.append(media)
            post['medias'] = medias
    
            posts.append({'id': post_id, 'text': post['text'], 'medias': medias})

        result['posts'] = posts
        result['object_access'] = object_access
        result['cache_status'] = lru_cache.get_status()
        #result['cache_status']['mem_usuage'] = asizeof.asizeof(lru_cache) 

    except Exception as ex:
        loggin.error(f'Exception: read request failed for {post_ids}')
        result['exception'] = type(ex).__name__
        raise NameError('Lets raise an exception for now')

    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result['timestamps'] = timestamps
    return result

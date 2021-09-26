import pymongo
from pymongo import MongoClient
import logging
from common.utils import get_timestamp_ms

mongo_client = None

def execute(args):
    global mongo_client
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
    if mongo_client is None:
        mongo_client = MongoClient(mongodb_ip_addr, mongodb_port)

    user_timeline_db = mongo_client['user_timeline']
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

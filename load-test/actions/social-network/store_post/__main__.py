import pymongo
from pymongo import MongoClient

from utils import get_timestamp_ms

mongo_client = None


def main(args):
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
    params = args.get('store_post', args)
    post = params['post']
    dbs = params['dbs']

    # -----------------------------------------------------------------------
    # Action execution
    # -----------------------------------------------------------------------
    mongodb_ip_addr = dbs['post_storage_mongodb']['ip_addr']
    mongodb_port = dbs['post_storage_mongodb']['port']
    if mongo_client is None:
        mongo_client = MongoClient(mongodb_ip_addr, mongodb_port)

    post_db = mongo_client['post']
    post_collection = post_db['post']
    post_collection.create_index([('post_id', pymongo.ASCENDING)],
                                 name='post_id', unique=True)
    post_collection.insert_one(post)

    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result = dict()
    result['timestamps'] = timestamps
    return result

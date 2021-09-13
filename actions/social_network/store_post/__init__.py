import pymongo
from pymongo import MongoClient
import logging

from actions.common.utils import get_timestamp_ms


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

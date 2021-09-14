import pymongo
from pymongo import MongoClient

from actions.common.utils import get_timestamp_ms, invoke_action
import logging

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
    params = args #.get('write_home_timeline', args)
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
    if mongo_client is None:
        mongo_client = MongoClient(mongodb_ip_addr, mongodb_port)

    home_timeline_db = mongo_client['home_timeline']
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
    return result

from pymongo import MongoClient

from actions.common.utils import get_timestamp_ms
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
    params = args.get('read_home_timeline', args)
    user_id = params['user_id']
    start = params['start']
    stop = params['stop']
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

    home_timeline = home_timeline_collection.find_one(
        filter={'user_id': user_id})
    post_ids = list()
    if home_timeline is not None:
        for post in home_timeline['posts']:
            post_ids.append(post['post_id'])
        if 0 <= start and start < stop:
            post_ids = post_ids[start:stop]

    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result = dict()
    result['timestamps'] = timestamps
    result['read_post'] = {
        'post_ids': post_ids,
        'dbs': dbs
    }
    logging.warning(f'read_home_timeline: user_id={user_id}, result is {result}')
    return result

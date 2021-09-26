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
    params = args #.get('read_user_timeline', args)
    user_id = params['user_id']
    start = params['start']
    stop = params['stop']
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

    # cache the post ids 
    user_timeline = user_timeline_collection.find_one(
        filter={'user_id': user_id})
    post_ids = list()
    if user_timeline is not None:
        for post in user_timeline['posts']:
            post_ids.append(post['post_id'])
        # sort the post according to ts and return the top x 
        if 0 <= start and start < stop and len(post_ids) >= stop:
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
    logging.warning(f'read_user_timeline: user_id={user_id}, result is {result}')
    return result

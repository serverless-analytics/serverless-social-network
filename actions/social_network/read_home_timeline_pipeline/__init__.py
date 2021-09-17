import random
import re
from threading import Thread
import logging

import aiohttp
import azure.functions as func
from requests.models import Response


from actions.common.utils import get_timestamp_ms, invoke_action



async def main(args):
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
    
    params = args #params = args.get('compose_post')
    user_id = params['user_id']
    start = params['start']
    stop = params['stop']
    dbs = params['dbs']

    # -----------------------------------------------------------------------
    # Action execution
    # -----------------------------------------------------------------------
    # construct post
    response = await invoke_action(action_name = 'read_home_timeline',
        params = {
            'read_home_timeline': {
                'user_id': user_id,
                'start': start,
                'stop': stop,
                'dbs': dbs
            }
        },
        blocking = True,
        result = True)


    # make this one parallel instead of one post per post id 
    post_timestamp = get_timestamp_ms()
    post_ids = response['read_post']['post_ids'] 
    response_read = {'posts': []}
    for post_id in post_ids:
        res = await invoke_action(action_name = 'read_post',
                params= {
                    'read_post': {
                        'user_id': user_id,
                        'post_ids': [post_id],
                        'timestamp': post_timestamp,
                        'dbs': dbs
                        }, #locality = postid, 3-->4 
                    },
                blocking = True,
                poll_interval = 0.1,
                result= True)
        response_read['posts'].append(res)
    
    #post_timestamp = get_timestamp_ms()
    #response_read = await invoke_action(action_name = 'read_post',
    #    params= {
    #        'read_post': response['read_post'],
    #    },
    #    blocking = True,
    #    result=True)


    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result = dict()
    result['timestamps'] = timestamps
    result['post_id'] = response['read_post']['post_ids']
    result['posts'] = response_read['posts']
    logging.warning(f'read_home_timeline_pipeline: user_id {user_id}, result is {result}')
    return result

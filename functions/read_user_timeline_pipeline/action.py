import random
import re
from threading import Thread
import logging

import aiohttp
import azure.functions as func
from requests.models import Response

from common.utils import get_timestamp_ms, invoke_action
import ast
import json


async def execute(args):
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
    params = args.get('read_user_timeline', args)
    
    request_id = args.get('request_id', -1)
    user_id = params['user_id']
    start = params['start']
    stop = params['stop']
    dbs = params['dbs']
    
    result = dict()
    # -----------------------------------------------------------------------
    # Action execution
    # -----------------------------------------------------------------------
    try: 
        response = await invoke_action(action_name = 'read_user_timeline',
            params = {
                'read_user_timeline': {
                    'user_id': user_id,
                    'start': start,
                    'stop': stop,
                    'dbs': dbs
                }
            }, # locality=userid, 
            blocking = True,
            poll_interval = 0.1,
            result = True)
        response = json.loads(response)
        result['post_ids'] = response['read_post']['post_ids']
        result['timeline'] = response
    

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
                    locality = post_id,
                    blocking = True,
                    poll_interval = 0.1,
                    result= True)
            read_posts = json.loads(res)
            response_read['posts'].append(read_posts)
        result['posts'] = response_read['posts']
        logging.warning(f'read_user_timeline_pipeline: user_id={user_id}, result is {result["post_ids"]}')

    except Exception as ex:
        logging.error(f'Exception read_user_timeline_pipeline failed, error: {type(ex).__name__}')
        result['exception'] = type(ex).__name__
    
    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result['request_id'] = request_id
    result['timestamps'] = timestamps
    return result

import random
import re
from threading import Thread
import logging

import aiohttp
import azure.functions as func
from requests.models import Response


from common.utils import get_timestamp_ms, invoke_action



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
    
    params = args #params = args.get('compose_post')
    user_id = params['user_id']
    post_id = params['post_id']
    post_timestamp = params['post_timestamp']
    user_mention_names = params['user_mention_names']
    dbs = params['dbs']

    # -----------------------------------------------------------------------
    # Action execution
    # -----------------------------------------------------------------------
    # construct post
    
    response = await invoke_action(action_name = 'read_social_graph',
        params = {
            'read_social_graph': {
                'user_id': user_id,
                'post_id': post_id,
                'post_timestamp': post_timestamp,
                'user_mention_names': user_mention_names,
                'dbs': dbs,
                'locality': user_id
            }
        },
        locality = user_id,
        blocking = True,
        poll_interval = 0.1,
        result=True)


    home_timeline_ids = response['write_home_timeline']['home_timeline_ids']
    logging.warning(f'read_social_graph response is {response}')
    response_write = await invoke_action(action_name = 'write_home_timeline',
        params= response,
        blocking = True,
        poll_interval = 0.1)


    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result = dict()
    result['timestamps'] = timestamps
    result['post_id'] = post_id
    result['user_mention_names'] = user_mention_names
    return result

import random
import re
from threading import Thread

from utils import get_timestamp_ms, invoke_action


def main(args):
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
    params = args.get('compose_post', args)
    username = params['username']
    user_id = params['user_id']
    text = params['text']
    media_ids = params['media_ids']
    media_types = params['media_types']
    post_type = params['post_type']
    dbs = params['dbs']

    # -----------------------------------------------------------------------
    # Action execution
    # -----------------------------------------------------------------------
    # construct post
    post_timestamp = get_timestamp_ms()
    post_id = random.getrandbits(63)
    author = {
        'user_id': user_id,
        'username': username
    }
    medias = list()
    for i in range(len(media_ids)):
        medias.append({
            'media_id': media_ids[i],
            'media_type': media_types[i]
        })
    post = {
        'post_id': post_id,
        'author': author,
        'text': text,
        'medias': medias,
        'timestamp': post_timestamp,
        'post_type': post_type
    }

    # parse user mentions
    user_mention_names = [username[1:]
                          for username in re.findall('@[a-zA-Z0-9-_]+', text)]

    # couchdb_client = CouchDB(user='whisk_admin',
    #                          auth_token=DB_PASSWORD,
    #                          url=DB_PROTOCOL + '://' + DB_HOST + ':' + DB_PORT,
    #                          connect=True)

    # invoke pipelines
    threads = []
    t = Thread(target=invoke_action, kwargs={
        'action_name': 'store_post',
        'params': {
            'store_post': {
                'post': post,
                'dbs': dbs
            }
        },
        'blocking': True,
        'poll_interval': 0.1
    })
    t.start()
    threads.append(t)

    t = Thread(target=invoke_action, kwargs={
        'action_name': 'write_user_timeline',
        'params': {
            'write_user_timeline': {
                'user_id': user_id,
                'post_id': post_id,
                'timestamp': post_timestamp,
                'dbs': dbs
            }
        },
        'blocking': True,
        'poll_interval': 0.1
    })
    t.start()
    threads.append(t)

    t = Thread(target=invoke_action, kwargs={
        'action_name': 'write_home_timeline_pipeline',
        'params': {
            'read_social_graph': {
                'user_id': user_id,
                'post_id': post_id,
                'post_timestamp': post_timestamp,
                'user_mention_names': user_mention_names,
                'dbs': dbs
            }
        },
        'blocking': True,
        'poll_interval': 0.1
    })
    t.start()
    threads.append(t)

    for t in threads:
        t.join()

    # -----------------------------------------------------------------------
    # Return results
    # -----------------------------------------------------------------------
    timestamps['main_end_ms'] = get_timestamp_ms()
    result = dict()
    result['timestamps'] = timestamps
    result['post_id'] = post_id
    result['user_mention_names'] = user_mention_names
    return result

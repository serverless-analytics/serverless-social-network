import logging
import json

import azure.functions as func

import actions.social_network.read_post as read_post

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        params = req_body.get('read_posts')
    except ValueError:
        raise NameError('This is the error you should catch')
    finally:
            result = read_post.main(params) if params else None

    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)
    

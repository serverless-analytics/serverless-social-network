import logging
import json
import azure.functions as func
import aiohttp

import actions.social_network.compose_post as compose_post


async def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        params = req_body.get('compose_post')
    except ValueError:
        raise NameError('This is the error you should catch')
    finally:
        result = (await compose_post.main(params)) if params else {}

    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)
    

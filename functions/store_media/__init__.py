import logging
import json

import azure.functions as func

#import actions.social_network.store_media as store_media

from . import action


def main(req: func.HttpRequest) -> func.HttpResponse:
    #logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
        params = req_body.get('store_media')
    except ValueError:
        raise NameError('This is the error you should catch')
    finally:
        #logging.warning(f'***** store_post -----> param is {params}')
        #result = store_media.main(params) if params else None
        result = action.execute(params) if params else None

    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)

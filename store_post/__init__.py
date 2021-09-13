import logging
import json

import azure.functions as func

import actions.social_network.store_post as store_post



def main(req: func.HttpRequest) -> func.HttpResponse:
    #logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
        params = req_body.get('store_post')
    except ValueError:
        raise NameError('This is the error you should catch')
    finally:
        #logging.warning(f'***** store_post -----> param is {params}')
        result = store_post.main(params) if params else None

    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)

import logging
import json

import azure.functions as func

from . import action


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger for {store_post} function processed a request.')

    try:
        req_body = req.get_json()
        params = req_body.get('store_post')
    except ValueError:
        raise NameError('This is the error you should catch')
    finally:
        result = action.execute(params) if params else None

    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)

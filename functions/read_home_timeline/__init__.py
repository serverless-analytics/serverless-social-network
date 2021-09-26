import logging

import azure.functions as func
import json
#import actions.social_network.read_home_timeline as read_home_timeline

from . import action

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        params = req_body.get('read_home_timeline')
    except ValueError:
        raise NameError('This is the error you should catch')
    finally:
        #result = read_home_timeline.main(params) if params else None
        result = action.execute(params) if params else None

    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)

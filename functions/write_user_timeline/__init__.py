import logging
import json
import azure.functions as func


#import actions.social_network.write_user_timeline as write_user_timeline
from . import action

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        params = req_body.get('write_user_timeline')
    except ValueError:
        raise NameError('This is the error you should catch')
    finally:
        #result = write_user_timeline.main(params) if params else None
        result = action.execute(params) if params else None

    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)

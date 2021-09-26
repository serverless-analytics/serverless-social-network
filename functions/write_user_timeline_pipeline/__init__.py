import logging
import json
import azure.functions as func

#import actions.social_network.write_user_timeline_pipeline as write_user_timeline_pipeline

from . import action

async def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        params = req_body.get('write_user_timeline_pipeline')
    except ValueError:
        raise NameError('This is the error you should catch')
    finally:
        #result = (await write_user_timeline_pipeline.main(params)) if params else {}
        result = (await action.execute(params)) if params else None

    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)

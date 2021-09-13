import logging
import json
import azure.functions as func
import actions.social_network.read_user_timeline_pipeline as read_user_timeline_pipeline

async def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        params = req_body.get('read_user_timeline')
    except ValueError:
        raise NameError('This is the error you should catch')
    finally:
        result = (await read_user_timeline_pipeline.main(params)) if params else {}

    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)

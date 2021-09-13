import logging
import json
import azure.functions as func
import actions.social_network.read_user_timeline as read_user_timeline


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        params = req_body.get('read_user_timeline')
    except ValueError:
        raise NameError('This is the error you should catch')
    finally:
        result = read_user_timeline.main(params) if params else None

    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)

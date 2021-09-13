import logging
import json
import azure.functions as func
import actions.social_network.write_home_timeline as write_home_timeline

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        params = req_body.get('write_home_timeline')
    except ValueError:
        raise NameError('This is the error you should catch')
    finally:
        result = write_home_timeline.main(params) if params else None

    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)

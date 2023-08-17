from flask import g

from model.api_history_dao import ApiHistoryDao as ah
from util.validator import validator

import json
import uuid

def before_request(request):
    g.history_id = uuid.uuid1()
    addr = request.remote_addr
    method = request.method
    path = request.path

    api_history = {
        'id' : g.get('history_id'),
        'group_id' : 'group001',
        'department_id' : 'department001',
        'user_id' : 'user001',
        'ip' : addr,
        'method' : method,
        'path' : path
    }

    if '/model/train' in path:
        params = dict(request.form)
        params_str = json.dumps(params, ensure_ascii = False)
        api_history.update({'params' : params_str})
    else:
        if method == 'POST':
            params = request.get_json(force = True).copy()
            if 'dataset' in params.keys():
                params.pop('dataset')
            if 'dataset_dtypes' in params.keys():
                params.pop('dataset_dtypes')
            params_str = json.dumps(params, ensure_ascii = False)
            api_history.update({'params' : params_str})
        
    ah.insert_api_history(api_history)
    
    payload_vldtr = validator(path)
    if payload_vldtr and not payload_vldtr.validate(params):
        raise Exception(payload_vldtr.errors)

def after_requset(response):
    api_history = {
        'id' : g.get('history_id'),
        'response' : response.status_code
    }

    ah.update_api_history(api_history)
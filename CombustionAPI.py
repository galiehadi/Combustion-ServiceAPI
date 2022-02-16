from distutils.log import debug
from flask import Flask, request, jsonify
from flask_cors import CORS
from UiService import *
from BackgroundService import *
import logging

# log = logging.getLogger('werkzeug')
# log.setLevel(logging.ERROR)
app = Flask(__name__)
cors = CORS(app, supports_credentials=True)
debug_mode = False

# ================================== Service UI ================================== #
@app.route('/service/copt/bat/combustion/indicator')
def indicator():
    data = {
        'message': 'Failed',
        'total': 1,
        'limit': 1,
        'page': 0,
    }
    data = jsonify(data)
    data.headers.add('Access-Control-Allow-Origin', '*')
    try:
        data['object'] = get_indicator()
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return jsonify(data)

@app.route('/service/copt/bat/combustion/alarm-history')
def alarm_history():
    page = request.args.get('page')
    limit = request.args.get('limit')

    data = {
        "message": "Failed",
        "total": 100,
        "limit": limit,
        "page": page
    }
    
    try:
        data['object'] = get_alarm_history(page,limit)
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/rule/<ruleID>')
def rule(ruleID):
    data = {
        "message": "Failed",
        "total": 1,
        "limit": 1,
        "page": 0
    }

    try:
        data['object'] = get_rules_detailed(ruleID)
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/tags/rule')
def tags_rule():
    data = {
        "message": "Failed",
        "total": 1,
        "limit": 1,
        "page": 0
    }

    try:
        data['object'] = get_tags_rule()
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/parameter/<parameterID>')
def parameter(parameterID):
    data = {
        "message": "Failed",
        "total": 1,
        "limit": 1,
        "page": 0
    }

    try:
        data['object'] = get_parameter_detailed(parameterID)
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data
    
@app.route('/service/copt/bat/combustion/rule', methods=['POST'])
def input_rule():
    payload = dict(request.get_json())

    objects = post_rule(payload)

    data = {
        "message": "Success" if (objects['Status'] == "Success") else "Failed",
        "total": 1,
        "limit": 1,
        "page": 0,
        "object": objects
    }

    return data

@app.route('/service/copt/bat/combustion/parameter', methods=['POST'])
def input_parameter():
    payload = dict(request.get_json())

    data = {
        "message": "Success",
        "total": 1,
        "limit": 1,
        "page": 0,
        "object": post_parameter(payload)
    }

    return data

# ================================== Background Service ================================== #
@app.route('/service/copt/bat/combustion/background/safeguardcheck')
def safeguard_check():
    data = {
        "message": "Failed",
        "total": 1,
        "limit": 1,
        "page": 0
    }

    try:
        data['object'] = bg_safeguard_update()
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/background/get_recom_exec_interval')
def get_recom_exec_interval():
    data = {
        "message": "Failed",
        "total": 1,
        "limit": 1,
        "page": 0
    }

    try:
        data['object'] = bg_get_recom_exec_interval()
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/background/update_machine_learning_recommendation')
def get_ml_recommendation():
    data = {
        "message": "Failed",
        "total": 1,
        "limit": 1,
        "page": 0
    }

    try:
        data['object'] = bg_get_ml_recommendation()
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

# ================================== Machine Learning Service ================================== #
@app.route('/service/copt/bat/combustion/background/runner')
def ml_runner():
    data = {
        "message": "Failed",
        "total": 1,
        "limit": 1,
        "page": 0
    }

    try:
        data['object'] = bg_ml_runner()
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data



if __name__ == '__main__':
    if debug_mode:
        app.run('0.0.0.0', port=8083, debug=True)
    else:
        app.run('0.0.0.0', port=8083, debug=False)
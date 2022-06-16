from distutils.log import debug
from flask import Flask, request, jsonify, make_response, send_file
from flask_cors import CORS, cross_origin
from itsdangerous import json
from numpy import asanyarray
from UiService import *
from BackgroundService import *
import logging, traceback

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
    try:
        data['object'] = get_indicator()
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    data = jsonify(data)
    data.headers.add('Access-Control-Allow-Origin', '*')
    return data

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
        data['total'] = len(data['object'])
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/detail/alarm-history/<alarmID>')
def alarm_history_id(alarmID):
    page = request.args.get('page')
    limit = request.args.get('limit')

    data = {
        "message": "Failed",
        "total": 100,
        "limit": limit,
        "page": page
    }
    
    try:
        data['object'] = get_specific_alarm_history(alarmID)
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/update/alarm-history/<alarmID>', methods=['POST'])
def alarm_history_post(alarmID):
    payload = dict(request.get_json())

    objects = post_alarm(payload)

    data = {
        "message": "Success" if (objects['Status'] == "Success") else "Failed",
        "total": 1,
        "limit": 1,
        "page": 0,
        "object": objects
    }

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
    data = jsonify(data)
    if (objects['Status'] != "Success"):
        return make_response(jsonify(data), 404)
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

@app.route('/service/copt/bat/combustion/export/<kind>', methods=['GET'])
def export_to_file(kind):
    kinds = ['recommendation','parameter-settings','rules-settings','alarm-history']
    if kind not in kinds: return make_response(f'"{kind}" not found. Please use one of {kinds}', 404)

    if kind == 'recommendation':
        filepath = get_recommendations(sql_interval='7 DAY', download=True)
    elif kind == 'parameter-settings':
        filepath = get_all_parameter()
    elif kind == 'rules-settings':
        filepath = get_all_rules_detailed()
    elif kind == 'alarm-history':
        filepath = get_alarm_history(0, 400, download=True)
    else:
        return f'"{kind}" not found. Please use one of {kinds}'
    return send_file(filepath, as_attachment=True)
    

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
        
        # sisipan
        bg_update_notification()
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
        data['message'] = str(traceback.format_exc())
    return data



if __name__ == '__main__':
    if debug_mode:
        app.run('0.0.0.0', port=8083, debug=True)
    else:
        app.run('0.0.0.0', port=8083, debug=False)

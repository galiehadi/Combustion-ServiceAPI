# from distutils.log import debug
from flask import Flask, request, jsonify, Response, send_file
from flask_cors import CORS
# from itsdangerous import json
from UiService import *
from BackgroundService import *
from flask import Flask,send_from_directory
import flask_excel as excel
import pyexcel as p

from datetime import datetime
import threading
from pyexcel.sheet import Sheet
from pyexcel_webio import make_response

_SLEEP_TIME_ = 2
_SLEEP_TIME_ML = 60
_SLEEP_TIME_ALARM_ = 30

# import logging
# import threading

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
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/detail/alarm-history/<alarmId>')
def alarm_history_detail(alarmId):
    data = {
        "message": "Failed",
        "total": 1,
        "limit": 1,
        "page": 1
    }
    
    try:
        data['object'] = get_alarm_history_detail(alarmId)
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/update/alarm-history', methods=['POST'])
def update_alarm_history():
    payload = dict(request.get_json())

    objects = update_alarm_desc(payload)

    data = {
        "message": "Success" if (objects['Status'] == "Success") else "Failed",
        "total": 1,
        "limit": 1,
        "page": 0,
        "object": objects
    }

    return data

@app.route('/service/copt/bat/combustion/export/alarm-history')
def export_alarm_history():
    start = request.args.get('startDate')
    end = request.args.get('endDate')

    data = {
        "message": "Failed",
        "total": 0,
        "start": start,
        "end": end
    }
    
    try:
        # filename = db_config._UNIT_NAME_ + '_COMBUSTION-ALARM HISTORY.xlsx'
        # resp = Response(get_export_alarm_history(start,end), mimetype='application/octet-stream')
        # resp.headers['Content-Disposition'] = 'attachment; filename="%s"' % filename
        # return resp
        # book = {'Alarm History': get_export_alarm_history(start, end)}
        # return excel.make_response_from_book_dict(book, 'xlsx', file_name=db_config._UNIT_NAME_ + '_COMBUSTION-ALARM HISTORY')
        return excel.make_response_from_array(get_export_alarm_history(start, end), 'csv', file_name=db_config._UNIT_NAME_ + '_COMBUSTION-ALARM HISTORY')
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

# Exporter Ali
# @app.route('/service/copt/bat/combustion/export/recommendation')
# def export_reccomendation():
#     data = {
#         "message": "Failed",
#         "total": 0,
#     }
    
#     try:
#         # filename = db_config._UNIT_NAME_ + '_COMBUSTION-RECOMMENDATION.xlsx'
#         # resp = Response(get_export_recommendation(), mimetype='application/octet-stream')
#         # resp.headers['Content-Disposition'] = 'attachment; filename="%s"' % filename
#         # return resp
#         # book = {'Recommendation': get_export_recommendation()}
#         # return excel.make_response_from_book_dict(book, 'xlsx', file_name=db_config._UNIT_NAME_ + '_COMBUSTION-RECOMMENDATION')
#         return excel.make_response_from_array(get_export_recommendation(), 'csv', file_name=db_config._UNIT_NAME_ + '_COMBUSTION-RECOMMENDATION')
#     except Exception as E:
#         data['object'] = []
#         data['message'] = str(E)
#     return data

# Exporter Ich
@app.route('/service/copt/bat/combustion/export/recommendation')
def export_reccomendation():
    data = {
        "message": "Failed",
        "total": 0,
    }

    try:
        payload = request.args.to_dict()
        filepath = get_recommendations(payload, sql_interval='7 DAY', download=True)
        print(filepath)
        return send_file(filepath, as_attachment=True)
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/export/parameter-settings')
def export_parameter_settings():
    data = {
        "message": "Failed",
        "total": 0,
    }
    
    try:
        # filename = db_config._UNIT_NAME_ + '_COMBUSTION-PARAMETER SETTINGS.xlsx'
        # resp = Response(get_export_parameter(), mimetype='application/octet-stream')
        # resp.headers['Content-Disposition'] = 'attachment; filename="%s"' % filename
        # return resp
        # book = {'Operation Parameter Settings': get_export_parameter()}
        # return excel.make_response_from_book_dict(book, 'xlsx', file_name=db_config._UNIT_NAME_ + '_COMBUSTION-PARAMETER SETTINGS')
        return excel.make_response_from_array(get_export_parameter(), 'csv', file_name=db_config._UNIT_NAME_ + '_COMBUSTION-PARAMETER SETTINGS')
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/export/rules-settings')
def export_rules_settings():
    data = {
        "message": "Failed",
        "total": 0,
    }
    
    try:
        # filename = db_config._UNIT_NAME_ + '_COMBUSTION-RULES SETTINGS.xlsx'
        # resp = Response(get_export_ruless(), mimetype='application/octet-stream')
        # resp.headers['Content-Disposition'] = 'attachment; filename="%s"' % filename
        # return resp
        # filename = db_config._UNIT_NAME_ + '_COMBUSTION-RULES SETTINGS.csv'
        # sheet = Sheet(get_export_rules())
        # output = excel.make_response(sheet, 'csv')
        # output.headers["Content-Disposition"] = 'attachment; filename="%s"' % filename
        # output.headers["Content-type"] = "text/csv"
        # return output
        # book = {'Rules Settings': get_export_rules()}
        # return excel.make_response_from_book_dict(book, 'csv', file_name=db_config._UNIT_NAME_ + '_COMBUSTION-RULES SETTINGS')
        return excel.make_response_from_array(get_export_rules(), 'csv', file_name=db_config._UNIT_NAME_ + '_COMBUSTION-RULES SETTINGS')
    except Exception as E:
        print(E)
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

@app.route('/service/copt/bat/combustion/background/alarmcheck')
def alarm_check():
    data = {
        "message": "Failed",
        "total": 1,
        "limit": 1,
        "page": 0
    }

    try:
        data['object'] = bg_alarm_runner()
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/background/permissivebiascek')
def perm_bias_check():
    data = {
        "message": "Failed",
        "total": 1,
        "limit": 1,
        "page": 0
    }

    try:
        data['object'] = update_permissive_bias()
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/background/writepvoxy')
def write_pv_oxy():
    data = {
        "message": "Failed",
        "total": 1,
        "limit": 1,
        "page": 0
    }

    try:
        data['object'] = write_smallest_pv_oxy()
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

@app.route('/service/copt/bat/combustion/background/coptokcheck')
def copt_on_check():
    data = {
        "message": "Failed",
        "total": 1,
        "limit": 1,
        "page": 0
    }

    try:
        data['object'] = copt_ok_check()
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

@app.route('/service/copt/bat/combustion/background/testwrite/<tag>/<value>')
def testwrite(tag, value):
    data = {
        "message": "Failed",
        "total": 1,
        "limit": 1,
        "page": 0
    }

    try:
        data['object'] = test_write_tag(tag, value)
        data['message'] = 'Success'
    except Exception as E:
        data['object'] = []
        data['message'] = str(E)
    return data

def t_safeguard_runner():
    while True:
        bg_safeguard_update()
        time.sleep(_SLEEP_TIME_)
        
def t_copt_enable_runner():
    while True:
        copt_ok_check()
        time.sleep(_SLEEP_TIME_)
        
def t_permissive_runner():
    while True:
        update_permissive_bias()
        time.sleep(_SLEEP_TIME_)

def t_alarm_runner():
    while True:
        bg_alarm_runner()
        time.sleep(_SLEEP_TIME_ALARM_)
        
def t_pv_oxy_runner():
    while True:
        write_smallest_pv_oxy()
        time.sleep(_SLEEP_TIME_)
        
def t_model_runner():
    while True:
        bg_ml_runner()
        time.sleep(_SLEEP_TIME_ML)
    

if __name__ == '__main__':
    sg_dcs_indicator_runner = threading.Thread(target=bg_safeguard_dcs_indicator, daemon=True)
    x2_o2_true_runner = threading.Thread(target=bg_maintain_x2_o2_true, daemon=True)
    # x2_o2l_true_runner = threading.Thread(target=bg_maintain_x2_o2l_true, daemon=True)
    # x2_o2r_true_runner = threading.Thread(target=bg_maintain_x2_o2r_true, daemon=True)
    safeguard_runner = threading.Thread(target=t_safeguard_runner, daemon=True)
    copt_enable_runner = threading.Thread(target=t_copt_enable_runner, daemon=True)
    permissive_runner = threading.Thread(target=t_permissive_runner, daemon=True)
    alarm_runner = threading.Thread(target=t_alarm_runner, daemon=True)
    pv_oxy_runner = threading.Thread(target=t_pv_oxy_runner, daemon=True)
    model_runner = threading.Thread(target=t_model_runner, daemon=True)
    
    sg_dcs_indicator_runner.start()
    x2_o2_true_runner.start()
    # x2_o2l_true_runner.start()
    # x2_o2r_true_runner.start()
    safeguard_runner.start()
    copt_enable_runner.start()
    permissive_runner.start()
    alarm_runner.start()
    pv_oxy_runner.start()
    model_runner.start()
    
    
    excel.init_excel(app)
    
    if debug_mode:
        app.run('0.0.0.0', port=8083, debug=True)
    else:
        app.run('0.0.0.0', port=8083, debug=False)

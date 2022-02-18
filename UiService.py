import pandas as pd
import numpy as np
import time, config
from urllib.parse import quote_plus as urlparse
from pprint import pprint

import sqlalchemy

_USER_ = config._USER_
_PASS_ = urlparse(config._PASS_)
_IP_ = config._IP_
_DB_NAME_ = config._DB_NAME_

con = f"mysql+mysqlconnector://{_USER_}:{_PASS_}@{_IP_}/{_DB_NAME_}"

def get_status():
    keys = [config.WATCHDOG_TAG,config.SAFEGUARD_TAG,'TAG:ENABLE_COPT']
    q = f"""SELECT f_tag_name FROM {_DB_NAME_}.tb_tags_read_conf ttrc 
            WHERE f_description = "Tag Enable COPT" """
    try: keys[2] = pd.read_sql(q, con).values[0][0]
    except Exception as e: print(f'Error on line 21: {e}')
    
    q = f"""SELECT f_address_no, f_value FROM {_DB_NAME_}.tb_bat_raw tbr 
            WHERE f_address_no IN {tuple(keys)}"""
    df = pd.read_sql(q, con)
    status = {}
    for k in keys:
        if k in df['f_address_no'].values: status[k] = df[df['f_address_no'] == k]['f_value'].values[0]
        else: status[k] = None
    return status[keys[0]], status[keys[1]], status[keys[2]]

def get_comb_tags():
    q = f"""SELECT cd.f_desc, tbr.f_value, cd.f_units FROM {_DB_NAME_}.cb_display cd 
            LEFT JOIN {_DB_NAME_}.tb_bat_raw tbr 
            ON cd.f_tags = tbr.f_address_no 
            ORDER BY cd.f_desc ASC"""
    df = pd.read_sql(q, con)
    df['f_value'] = df['f_value'].astype(str)
    df = df.replace('None',0)
    df = df.set_index('f_desc')
    df['f_value'] = df['f_value'].astype(float).round(2)
    df['f_value'] = df['f_value'].astype(str) + ' ' + df['f_units']
    df = df.to_dict()
    df = df['f_value']
    return df

def get_parameter():
    q = f"""SELECT f_parameter_id AS 'id', f_label AS 'label', f_default_value AS 'value' FROM {_DB_NAME_}.tb_combustion_parameters"""
    df = pd.read_sql(q, con)
    df = df.to_dict(orient='records')
    
    return df

def get_recommendations():
    q = f"""SELECT ts AS timestamp, tag_name AS 'desc', value AS targetValue, bias_value AS setValue, value-bias_value AS currentValue FROM {_DB_NAME_}.tb_combustion_model_generation
            WHERE ts > (SELECT ts FROM {_DB_NAME_}.tb_combustion_model_generation
                        GROUP BY ts ORDER BY ts DESC LIMIT 4, 1)
            AND ts > NOW() - INTERVAL 1 DAY
            ORDER BY ts DESC"""
    df = pd.read_sql(q, con)
    for c in df.columns[-3:]:
        df[c] = np.round(df[c], 3)
    df_dict = df.astype(str).to_dict('records')
    last_recommendation = str(df['timestamp'].max())
    
    return df_dict, last_recommendation

def get_rules_header():
    q = f"""SELECT f_rule_hdr_id AS id, f_rule_descr AS label FROM {_DB_NAME_}.tb_combustion_rules_hdr"""
    df = pd.read_sql(q, con)
    df_dict = df.to_dict('records')
    return df_dict

def get_alarm_history(page, limit):
    l1 = 0; l2 = 100
    if bool(page) and bool(limit):
        page = max([int(page),0]); limit = int(limit)
        l1 = (page) * limit
        l2 = (page+1) * limit
    q = f"""SELECT f_int_id AS alarmId, f_timestamp AS date, f_desc AS 'desc',
            f_set_value AS setValue, f_actual_value AS actualValue
            FROM {_DB_NAME_}.tb_combustion_alarm_history
            ORDER BY f_timestamp DESC
            LIMIT {l1},{l2}"""
    df = pd.read_sql(q, con)
    df_dict = df.astype(str).to_dict('records')
    return df_dict

def get_rules_detailed(rule_id):
    q = f"""SELECT f_rule_dtl_id AS ruleDetailId, f_rule_hdr_id AS ruleHeaderId, f_sequence AS sequence, f_bracket_open AS bracketOpen, f_bracket_close AS bracketClose, f_tag_sensor AS tagSensor 
            FROM {_DB_NAME_}.tb_combustion_rules_dtl
            WHERE f_rule_hdr_id = {rule_id} """
    df = pd.read_sql(q, con)
    df_dict = df.to_dict('records')
    ret = {
        'detailRule': df_dict
    }
    return ret

def get_tags_rule():
    q = f"""SELECT "" AS tagKKS, f_tag_name AS tagSensor, f_description AS tagDescription FROM {_DB_NAME_}.tb_tags_read_conf ttrc 
            WHERE f_tag_use IN ("COPT", "SOPT+COPT", "COPT+SOPT")
            AND f_is_active != 0"""
    df = pd.read_sql(q, con)
    df_dict = df.to_dict('records')
    return df_dict

def get_indicator():
    watchdog_status, safeguard_status, comb_enable_status = get_status()
    recommendations, last_recommendation = get_recommendations()
    comb_tags = get_comb_tags()
    parameter = get_parameter()
    rules = get_rules_header()
    object = {
        'watchdog': watchdog_status,
        'comb_enable': comb_enable_status, 
        'parameter': parameter,
        'last_recommendation': last_recommendation,
        'rules': rules,
        'comb_tags': comb_tags,
        'recommendations': recommendations,
        'safeguard': safeguard_status
    }
    return object

def get_parameter_detailed(parameter_id):
    q = f"""SELECT f_parameter_id AS id, f_label AS label, f_default_value AS value FROM {_DB_NAME_}.tb_combustion_parameters
            WHERE f_parameter_id = {parameter_id}"""
    df = pd.read_sql(q, con)
    if len(df) > 0:
        df_dict = df.to_dict('records')[0]
    else: df_dict = {}
    return df_dict

def post_rule(payload):
    ret = {'Status': 'Failed'}
    
    if type(payload) is not dict: 
        return ret
    if len(payload.keys()) == 0: 
        return ret
    if 'detailRule' not in payload.keys():
        return ret

    q = f"""INSERT INTO
			{_DB_NAME_}.tb_combustion_rules_dtl(f_rule_hdr_id, f_rule_descr, f_tag_sensor, f_rules, f_operator, f_unit, f_limit_high, f_limit_low, f_sequence, f_bracket_open, f_bracket_close, f_is_active, f_updated_at)
            VALUES """
    
    Payload = payload['detailRule']
    evaluate = ''
    tags_used = []
    for P in Payload:
        print(type(P), P)
        bracketOpen = P['bracketOpen']
        bracketClose = P['bracketClose']
        sequence = P['sequence']
        tagSensor = [P['tagSensor'][:-1].split('(')[0] if '(' in P['tagSensor'] else P['tagSensor']][0]
        if 'ruleHeaderId' in P.keys(): ruleHeaderId = P['ruleHeaderId']
        else: ruleHeaderId = 20

        r = f"""( {ruleHeaderId} , NULL, '{tagSensor}', NULL, NULL, NULL, NULL, NULL, {sequence}, '{bracketOpen}', '{bracketClose}', 1, NOW()),"""
        q += r
        evaluate += f"{bracketOpen}{tagSensor}{bracketClose} "
        tags_used.append(tagSensor)
        
    q = q[:-1]

    # Value check
    qcheck = f"SELECT f_address_no, f_value FROM {_DB_NAME_}.tb_bat_raw WHERE f_address_no IN {tuple(tags_used)}"
    df = pd.read_sql(qcheck, con)
    df = df.set_index('f_address_no')['f_value']

    for k in df.index: evaluate = evaluate.replace(k, str(df[k]))

    try:
        Safeguard_status = eval(evaluate.lower())
        qdel = f"""DELETE FROM {_DB_NAME_}.tb_combustion_rules_dtl
                   WHERE f_rule_hdr_id={ruleHeaderId}"""
        
        engine = sqlalchemy.create_engine(con)

        with engine.connect() as conn:
            red = conn.execute(qdel)
            res = conn.execute(q)
        
        return {'Status':'Success'}

    except Exception as E:
        print(E)
        print(evaluate)
        return {'Status': str(E)}

    

def post_parameter(payload):
    parameterID = payload['id']
    label = payload['label']
    defaultValue = payload['value']

    q = f"""INSERT INTO
			{_DB_NAME_}.tb_combustion_parameters (f_parameter_id,f_label,f_default_value,f_is_active,f_updated_at)
            VALUES ({parameterID},'{label}',{defaultValue},1,NOW());
         """
    qdel = f"""DELETE FROM {_DB_NAME_}.tb_combustion_parameters
	           WHERE f_parameter_id={parameterID}"""

    engine = sqlalchemy.create_engine(con)

    with engine.connect() as conn:
        red = conn.execute(qdel)
        res = conn.execute(q)

    return {'Status':'Success'}
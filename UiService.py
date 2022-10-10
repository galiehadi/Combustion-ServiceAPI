import pandas as pd
import numpy as np
import time, config, traceback, re, os
from BackgroundService import logging
from urllib.parse import quote_plus as urlparse
from pprint import pprint
from sqlalchemy import create_engine

_USER_ = config._USER_
_PASS_ = urlparse(config._PASS_)
_IP_ = config._IP_
_DB_NAME_ = config._DB_NAME_
_TEMP_FOLDER_ = config.TEMP_FOLDER

con = f"mysql+mysqlconnector://{_USER_}:{_PASS_}@{_IP_}/{_DB_NAME_}"
engine = create_engine(con)

def save_to_path(dataframe, filename="download"):
    if not os.path.isdir(_TEMP_FOLDER_): os.makedirs(_TEMP_FOLDER_)

    # Delete all old files
    files = [os.path.join(_TEMP_FOLDER_, f) for f in os.listdir(_TEMP_FOLDER_)]
    current_time = time.time()
    for file in files:
        try:
            if (current_time - os.path.getmtime(file)) > (60*60*24): os.remove(file)
        except Exception as E:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')} - {E}")

    # Save file, and return file path
    #filename = f"COPT-{filename}-{time.strftime('%Y-%m-%d %H%M%S')}.csv" # CSV file
    filename = f"COPT-{filename}-{time.strftime('%Y-%m-%d %H%M%S')}.xlsx" # Excel file
    path = os.path.join(_TEMP_FOLDER_, filename)
    dataframe.to_excel(path, index=False)
    return path

def get_status():
    keys = [config.WATCHDOG_TAG, config.SAFEGUARD_TAG, config.DESC_ENABLE_COPT]
    q = f"""SELECT conf.f_description AS f_address_no, raw.f_value FROM tb_bat_raw raw
        LEFT JOIN tb_tags_read_conf conf
        ON raw.f_address_no = conf.f_tag_name
        WHERE conf.f_description IN {tuple(keys)}
        UNION
        SELECT f_address_no, f_value FROM tb_bat_raw
        WHERE f_address_no IN {tuple(keys)}"""
    df = pd.read_sql(q, engine).replace(np.nan, 0)
    status = {}
    for k in keys:
        if k in df['f_address_no'].values: status[k] = int(df[df['f_address_no'] == k]['f_value'].values[0])
        elif np.isnan(status[k]): status[k] = int(0)
        else: status[k] = 0
    return status[keys[0]], status[keys[1]], status[keys[2]]

def get_o2_converter_parameters():
    try:
        q = f"""SELECT hdr.f_rule_hdr_id, hdr.f_rule_descr, dtl.f_tag_sensor, dtl.f_bracket_open, raw.f_value, dtl.f_bracket_close FROM tb_combustion_rules_hdr hdr
                LEFT JOIN tb_combustion_rules_dtl dtl 
                ON hdr.f_rule_hdr_id = dtl.f_rule_hdr_id 
                LEFT JOIN tb_bat_raw raw
                ON dtl.f_tag_sensor = raw.f_address_no 
                WHERE hdr.f_is_active = 1
                AND dtl.f_is_active = 1"""
        rules = pd.read_sql(q, engine)
        o2_a_params = rules[rules['f_rule_descr'] == 'O2_A_CALLIBRATION']['f_bracket_close'].values[0]
        o2_b_params = rules[rules['f_rule_descr'] == 'O2_B_CALLIBRATION']['f_bracket_close'].values[0]

        o2_a_intercept, o2_a_coef = [float(f.replace(' ','')) for f in re.findall('[-\s]+[0-9.]+', o2_a_params)]
        o2_b_intercept, o2_b_coef = [float(f.replace(' ','')) for f in re.findall('[-\s]+[0-9.]+', o2_b_params)]

        return np.average([o2_a_intercept, o2_b_intercept]), np.average([o2_a_coef, o2_b_coef])
    except:
        # logging('Failed to fetch o2 parameters. Giving out the default value ...')
        return [1, 0]

def get_comb_tags():
    q = f"""SELECT cd.f_desc, cd.f_data_type, tbr.f_value, cd.f_units FROM {_DB_NAME_}.cb_display cd 
            LEFT JOIN {_DB_NAME_}.tb_bat_raw tbr 
            ON cd.f_tags = tbr.f_address_no 
            ORDER BY cd.f_desc ASC"""
    df = pd.read_sql(q, engine)
    df['f_value'] = df['f_value'].astype(str)
    df = df.replace('None',0)
    df = df.set_index('f_desc')

    # df.loc['excess_o2','f_value']  = float(df.loc['excess_o2','f_value']) * 1.6844264 + 0.16792374

    o2_intercept, o2_bias = get_o2_converter_parameters()
    df.loc['excess_o2','f_value']  = float(df.loc['excess_o2','f_value']) * o2_intercept + o2_bias

    df['f_value'] = df['f_value'].astype(float).round(2).astype(str)
    text = []
    for f in df.index:
        is_boolean = df.loc[f, 'f_data_type'] == "bool"
        if is_boolean: text.append(bool(float(df.loc[f, 'f_value'])))
        else: text.append(df.loc[f, 'f_value'] + " " + df.loc[f, 'f_units'])
    df['text'] = text
    return df.to_dict()['text']

def get_parameter():
    q = f"""SELECT f_parameter_id AS 'id', f_label AS 'label', f_default_value AS 'value' FROM {_DB_NAME_}.tb_combustion_parameters"""
    df = pd.read_sql(q, engine)
    df = df.to_dict(orient='records')
    
    return df

def get_recommendations(payload = None, sql_interval = '1 DAY', download = False):
    if type(payload) == dict:
        endDate = pd.to_datetime('now').ceil('1d') 
        startDate = endDate - pd.to_timedelta('7 day')
        if 'startDate' in payload.keys():
            startDate = pd.to_datetime(payload['startDate'])
        if 'endDate' in payload.keys():
            endDate = pd.to_datetime(payload['endDate'])
    else:
        startDate, endDate = (pd.to_datetime('now') - pd.to_timedelta(sql_interval), pd.to_datetime('now'))

    if download:
        where_state = f"""WHERE ts BETWEEN "{startDate.strftime('%Y-%m-%d')}" AND "{endDate.strftime('%Y-%m-%d')}" """
    else:
        where_state = f"WHERE ts > (SELECT ts FROM {_DB_NAME_}.tb_combustion_model_generation GROUP BY ts ORDER BY ts DESC LIMIT 4, 1)"

    q = f"""SELECT ts AS timestamp, tag_name AS 'desc', value AS targetValue, bias_value AS setValue, value-bias_value AS currentValue FROM {_DB_NAME_}.tb_combustion_model_generation
            {where_state}
            ORDER BY ts DESC, tag_name ASC"""

    df = pd.read_sql(q, engine)
    if download:
        return save_to_path(df, "recommendation")

    else:
        for c in df.columns[-3:]:
            df[c] = np.round(df[c], 3)
        df_dict = df.astype(str).to_dict('records')
        
        last_recommendation = str(df['timestamp'].max())
        
        return df_dict, last_recommendation

def get_rules_header():
    q = f"""SELECT f_rule_hdr_id AS id, f_rule_descr AS label FROM {_DB_NAME_}.tb_combustion_rules_hdr"""
    df = pd.read_sql(q, engine)
    df_dict = df.to_dict('records')
    return df_dict

def get_alarm_history(page=0, limit=40, payload=None, download=False):
    l1 = 0; l2 = 100; LIMIT = ""; WHERE = ""
    if bool(page) or bool(limit):
        page = max([int(page),0]); limit = int(limit)
        l1 = (page) * limit
        l2 = (page+1) * limit
        WHERE = ""
        LIMIT = f"LIMIT {l1},{l2}"

    if type(payload) == dict:
        startDate = pd.to_datetime('now').ceil('1d') 
        endDate = startDate - pd.to_timedelta('1 day')
        if 'startDate' in payload.keys():
            startDate = pd.to_datetime(payload['startDate'])
        if 'endDate' in payload.keys():
            endDate = pd.to_datetime(payload['endDate'])
        WHERE = f"""WHERE f_timestamp BETWEEN "{startDate.strftime('%Y-%m-%d')}" AND "{endDate.strftime('%Y-%m-%d')}" """
        LIMIT = ""

    q = f"""SELECT f_int_id AS alarmId, f_timestamp AS date, f_desc AS 'desc',
            f_set_value AS setValue, f_actual_value AS actualValue
            FROM {_DB_NAME_}.tb_combustion_alarm_history
            {WHERE}
            ORDER BY f_timestamp DESC
            {LIMIT}"""
    df = pd.read_sql(q, engine)
    if download:
        return save_to_path(df)
    df_dict = df.astype(str).to_dict('records')
    return df_dict

def get_specific_alarm_history(alarmID):
    q = f"""SELECT f_int_id AS alarmId, f_timestamp AS `date`, 
            f_set_value AS setValue, f_actual_value AS actualValue, 
            f_desc AS `desc` FROM {_DB_NAME_}.tb_combustion_alarm_history tcah 
            WHERE f_int_id = {alarmID} """
    df = pd.read_sql(q, engine)
    df_dict = df.astype(str).to_dict('records')
    if len(df_dict) > 0:
        return df_dict[0]
    else:
        return {}

def get_rules_detailed(rule_id):
    q = f"""SELECT f_rule_dtl_id AS ruleDetailId, f_rule_hdr_id AS ruleHeaderId, f_sequence AS sequence, f_bracket_open AS bracketOpen, f_bracket_close AS bracketClose, f_tag_sensor AS tagSensor 
            FROM {_DB_NAME_}.tb_combustion_rules_dtl
            WHERE f_rule_hdr_id = {rule_id} """
    df = pd.read_sql(q, engine)
    df_dict = df.to_dict('records')
    ret = {
        'detailRule': df_dict
    }
    return ret

def get_all_rules_detailed():
    q = f"""SELECT
                hdr.f_rule_hdr_id AS `No`,
                hdr.f_rule_descr AS `Rule`,
                rule.f_sequence AS `Sequence`,
                conf.f_description AS Description,
                CONCAT(rule.f_bracket_open , rule.f_tag_sensor, rule.f_bracket_close ) AS RuleDetail,
                CONCAT(rule.f_bracket_open , raw.f_value, rule.f_bracket_close ) AS CurrentValue
            FROM
                {_DB_NAME_}.tb_combustion_rules_dtl rule
            LEFT JOIN {_DB_NAME_}.tb_bat_raw raw ON
                rule.f_tag_sensor = raw.f_address_no
            LEFT JOIN {_DB_NAME_}.tb_tags_read_conf conf ON
                rule.f_tag_sensor = conf.f_tag_name
            LEFT JOIN {_DB_NAME_}.tb_combustion_rules_hdr hdr ON
                hdr.f_rule_hdr_id = rule.f_rule_hdr_id
            WHERE
                hdr.f_rule_hdr_id > 0; """
    df = pd.read_sql(q, engine)
    return save_to_path(df, "rules")

def get_tags_rule():
    q = f"""SELECT "" AS tagKKS, f_tag_name AS tagSensor, 
            f_description AS tagDescription FROM tb_tags_read_conf ttrc 
            WHERE f_tag_use IN ("COPT", "SOPT+COPT", "COPT+SOPT")
            AND f_is_active != 0"""
    df = pd.read_sql(q, engine)
    df['tagDescription'] = [f.strip() for f in df['tagDescription'].astype(str)]
    df['tagDescription'] = df['tagSensor'] + ' -- ' + df['tagDescription']
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
    df = pd.read_sql(q, engine)
    if len(df) > 0:
        df_dict = df.to_dict('records')[0]
    else: df_dict = {}
    return df_dict

def get_all_parameter():
    q = f"""SELECT f_parameter_id AS id, f_label AS label, f_default_value AS value FROM {_DB_NAME_}.tb_combustion_parameters
            WHERE f_is_active = 1"""
    df = pd.read_sql(q, engine)
    return save_to_path(df, "parameters")


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
        bracketOpen = P['bracketOpen']
        bracketClose = P['bracketClose']
        sequence = P['sequence']
        # tagSensor = [P['tagSensor'][:-1].split('(')[0] if '(' in P['tagSensor'] else P['tagSensor']][0]
        tagSensor = P['tagSensor'].split(' -- ')[0] if ' -- ' in P['tagSensor'] else P['tagSensor']
        if 'ruleHeaderId' in P.keys(): ruleHeaderId = P['ruleHeaderId']
        else: ruleHeaderId = 20

        r = f"""( {ruleHeaderId} , NULL, '{tagSensor}', NULL, NULL, NULL, NULL, NULL, {sequence}, '{bracketOpen}', '{bracketClose}', 1, NOW()),"""
        q += r
        evaluate += f"{bracketOpen}{tagSensor}{bracketClose} "
        tags_used.append(tagSensor)
        
    q = q[:-1]

    # Value check
    wherescript = f"('{tags_used[0]}')" if len(tags_used) == 1 else tuple(tags_used)
    qcheck = f"SELECT f_address_no, f_value FROM {_DB_NAME_}.tb_bat_raw WHERE f_address_no IN {wherescript}"
    df = pd.read_sql(qcheck, engine)
    df = df.set_index('f_address_no')['f_value']

    for k in df.index: evaluate = evaluate.replace(k, str(df[k]))
    evaluate = evaluate.lower().replace("=","==")
    while "===" in evaluate: evaluate = evaluate.replace("===","==")

    try:
        Safeguard_status = eval(evaluate)
        qdel = f"""DELETE FROM {_DB_NAME_}.tb_combustion_rules_dtl
                   WHERE f_rule_hdr_id={ruleHeaderId}"""
        
        with engine.connect() as conn:
            red = conn.execute(qdel)
            res = conn.execute(q)
        
        return {'Status':'Success'}

    except Exception as E:
        logging(f"Error evaluating a new rule: {evaluate}")
        logging(f"{E}")
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

    with engine.connect() as conn:
        red = conn.execute(qdel)
        res = conn.execute(q)

    return {'Status':'Success'}

def post_alarm(payload):
    return {'Status': 'Failed'}

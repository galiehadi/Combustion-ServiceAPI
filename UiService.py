import pandas as pd
import numpy as np
import time, os
# from urllib.parse import quote_plus as urlparse
# from pprint import pprint
from BackgroundService import *

# import sqlalchemy
from sqlalchemy.engine import create_engine
# from unittest.mock import inplace

_USER_ = db_config._USER_
_PASS_ = urlparse(db_config._PASS_)
_IP_ = db_config._IP_
_DB_NAME_ = db_config._DB_NAME_
_TEMP_FOLDER_ = db_config.TEMP_FOLDER

con_str = f"mysql+mysqlconnector://{_USER_}:{_PASS_}@{_IP_}/{_DB_NAME_}"
con = create_engine(con_str, pool_size=10, max_overflow=0)

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
    filename = f"COPT-{filename}-{time.strftime('%Y-%m-%d %H%M%S')}.csv" # CSV file
    # filename = f"COPT-{filename}-{time.strftime('%Y-%m-%d %H%M%S')}.xlsx" # Excel file
    path = os.path.join(_TEMP_FOLDER_, filename)
    dataframe.to_csv(path, index=False)
    return path

def get_status():
    keys = get_constrains([db_config.SAFEGUARD_BAT, db_config.COPT_OK])
    # q = f"""SELECT f_tag_name FROM {_DB_NAME_}.tb_tags_read_conf ttrc 
    #         WHERE f_description = "Tag Enable COPT" """
    # try: keys[2] = pd.read_sql(q, con).values[0][0]
    # except Exception as e: print(f'Error on line 21: {e}')
    
    q = f"""SELECT f_address_no, f_value FROM {_DB_NAME_}.tb_bat_raw tbr 
            WHERE f_address_no IN {tuple(keys)}"""
    df = pd.read_sql(q, con)
    status = {}
    for k in keys:
        if k in df['f_address_no'].values: 
            status[k] = df[df['f_address_no'] == k]['f_value'].values[0]
            if status[k] == 'True':
                status[k] = 1
            if status[k] == 'False':
                status[k] = 0
        else: 
            status[k] = None
    return get_watchdog_indicator(), status[keys[0]], status[keys[1]]

def get_watchdog_indicator():
    q = f"""SELECT
                f_default_value
            FROM
                tb_sootblow_control tsc
            WHERE
                tsc.f_control_id = {db_config.WATCHDOG_INDICATOR}"""
    df = pd.read_sql(q, con)
    return df['f_default_value'][0]

def get_comb_tags():
    try:
        q = f"""SELECT cd.f_desc, IF (tbr.f_value is null or tbr.f_value = '' or tbr.f_value = 'False', 0, IF (tbr.f_value = 'True', 1, tbr.f_value)) as f_value, IF (cd.f_units is null, '', cd.f_units) as f_units FROM {_DB_NAME_}.cb_display cd 
                LEFT JOIN {_DB_NAME_}.tb_bat_raw tbr 
                ON cd.f_tags = tbr.f_address_no 
                ORDER BY cd.f_desc ASC"""
        df = pd.read_sql(q, con)
        df['f_value'] = df['f_value'].astype(str)
        # df = df.replace('',0)
        # df = df.replace('None',0)
        # df = df.replace('True',1)
        # df = df.replace('False',0)
        df['f_value'] = df['f_value'].astype(float).round(2)
        total_air_flow = df.loc[df['f_desc'].isin(['2a','2b','2c','2d','2e','2f','2g','2h'])]['f_value'].sum().round(2)
        df.loc[df['f_desc'] == '6b', 'f_value'] = total_air_flow
        df = df.set_index('f_desc')
        df['f_value'] = df['f_value'].astype(str) + ' ' + df['f_units']
        df = df.to_dict()
        df = df['f_value']
        return df
    except Exception as E:
        print(E)
    

def query_parameter():
    q = f"""SELECT f_parameter_id AS 'id', f_label AS 'label', f_default_value AS 'value' FROM {_DB_NAME_}.tb_combustion_parameters WHERE f_is_active = 1"""
    df = pd.read_sql(q, con)
    return df

def get_parameter():
    df = query_parameter()
    df = df.to_dict(orient='records')
    
    return df

# def get_export_parameter():
#     df = query_parameter()
#     df_dict = df.astype(str).to_dict('records')
#
#     if len(df_dict) > 0:
#         keys = df_dict[0].keys()
#         yield ','.join(keys) + '\n'
#
#         for row in df_dict:
#             yield ','.join(row.values()) + '\n'
#     else:
#         yield 'id,label,value'

def get_export_parameter():
    df = query_parameter()
    df_dict = df.astype(str).to_dict('records')
    master = []
    
    if len(df_dict) > 0:
        keys = df_dict[0].keys()
        master.append(list(keys))
        
        for row in df_dict:
            master.append(list(row.values()))
    else:
        master.append(['id','label','value'])
    
    return master

# Exporter Ali
# def get_recommendations():
#     q = f"""SELECT ts AS timestamp, tag_name AS 'desc', value AS targetValue, bias_value AS setValue, value-bias_value AS currentValue FROM {_DB_NAME_}.tb_combustion_model_generation
#             WHERE ts > (SELECT ts FROM {_DB_NAME_}.tb_combustion_model_generation
#                         GROUP BY ts ORDER BY ts DESC LIMIT 4, 1)
#             AND ts > NOW() - INTERVAL 1 DAY
#             ORDER BY ts DESC"""
#     df = pd.read_sql(q, con)
#     for c in df.columns[-3:]:
#         df[c] = np.round(df[c], 3)
#     df_dict = df.astype(str).to_dict('records')
#     last_recommendation = str(df['timestamp'].max())
    
#     return df_dict, last_recommendation

# Exporter Ich
def get_recommendations(payload = None, sql_interval = '1 DAY', download = False):
    if type(payload) == dict:
        endDate = pd.to_datetime('now').ceil('1d') 
        startDate = endDate - pd.to_timedelta('90 day')
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

# def get_export_recommendation():
#     df_dict, last_recommendation = get_recommendations()
#
#     if len(df_dict) > 0:
#         keys = df_dict[0].keys()
#         yield ','.join(keys) + '\n'
#
#         for row in df_dict:
#             yield ','.join(row.values()) + '\n'
#     else:
#         yield 'timestamp,desc,targetValue,setValue,currentValue'

def get_export_recommendation():
    df_dict, last_recommendation = get_recommendations()
    master = []
    
    if df_dict != None and len(df_dict) > 0:
        keys = df_dict[0].keys()
        master.append(list(keys))
        
        for row in df_dict:
            master.append(list(row.values()))
    else:
        master.append(['timestamp','desc','targetValue','setValue','currentValue'])
    
    return master;

def get_rules_header():
    q = f"""SELECT f_rule_hdr_id AS id, f_rule_descr AS label FROM {_DB_NAME_}.tb_combustion_rules_hdr"""
    df = pd.read_sql(q, con)
    df_dict = df.to_dict('records')
    return df_dict

# def get_export_rules():
#     q = f"""SELECT
#                 tcrh.f_rule_hdr_id AS `No`,
#                 tcrh.f_rule_descr AS Rule,
#                 tcrd.f_sequence AS `Sequence`,
#                 ttrc.f_description AS Description,
#                 CONCAT(tcrd.f_bracket_open, tcrd.f_tag_sensor, tcrd.f_bracket_close) AS RuleDetail,
#                 CONCAT(tcrd.f_bracket_open, tbr.f_value, tcrd.f_bracket_close) AS CurrentValue
#             FROM
#                 tb_combustion_rules_dtl tcrd
#             JOIN tb_combustion_rules_hdr tcrh ON
#                 tcrh.f_rule_hdr_id = tcrd.f_rule_hdr_id
#             LEFT JOIN tb_tags_read_conf ttrc ON
#                 ttrc.f_tag_name = tcrd.f_tag_sensor
#             JOIN tb_bat_raw tbr ON
#                 tbr.f_address_no = ttrc.f_tag_name
#             ORDER BY
#                 tcrh.f_rule_hdr_id ASC,
#                 tcrd.f_sequence ASC"""
#     df = pd.read_sql(q, con)
#     df_dict = df.astype(str).to_dict('records')
#
#     if len(df_dict) > 0:
#         keys = df_dict[0].keys()
#         yield ','.join(keys) + '\n'
#
#         for row in df_dict:
#             yield ','.join(row.values()) + '\n'
#     else:
#         yield 'No,Rule,Sequence,Description,RuleDetail,CurrentValue'
        
def get_export_rules():
    q = f"""SELECT
                tcrh.f_rule_hdr_id AS `No`,
                tcrh.f_rule_descr AS Rule,
                tcrd.f_sequence AS `Sequence`,
                ttrc.f_description AS Description,
                CONCAT(tcrd.f_bracket_open, tcrd.f_tag_sensor, tcrd.f_bracket_close) AS RuleDetail,
                CONCAT(tcrd.f_bracket_open, tbr.f_value, tcrd.f_bracket_close) AS CurrentValue
            FROM
                tb_combustion_rules_dtl tcrd
            JOIN tb_combustion_rules_hdr tcrh ON
                tcrh.f_rule_hdr_id = tcrd.f_rule_hdr_id
            LEFT JOIN tb_tags_read_conf ttrc ON
                ttrc.f_tag_name = tcrd.f_tag_sensor
            JOIN tb_bat_raw tbr ON
                tbr.f_address_no = ttrc.f_tag_name
            ORDER BY
                tcrh.f_rule_hdr_id ASC,
                tcrd.f_sequence ASC"""
    df = pd.read_sql(q, con)
    df_dict = df.astype(str).to_dict('records')
    master = []
    concated_rules = ''
    
    if len(df_dict) > 0:
        keys = df_dict[0].keys()
        master.append(list(keys))
        
        for row in df_dict:
            master.append(list(row.values()))
            concated_rules += row['RuleDetail']
    else:
        master.append(['No','Rule','Sequence','Description','RuleDetail','CurrentValue'])
    
    master.append([concated_rules])
    
    return master

def get_alarm_history(page, limit):
    l1 = 0; l2 = 100
    if bool(page) and bool(limit):
        page = max([int(page),0]); limit = int(limit)
        l1 = (page) * limit
        l2 = (page+1) * limit
    q = f"""SELECT * FROM {_DB_NAME_}.tb_combustion_alarm_history
            ORDER BY f_timestamp DESC
            LIMIT {l1},{l2}"""
    # q = f"""SELECT ts AS f_timestamp, message AS f_desc, status AS f_set_value FROM {_DB_NAME_}.tb_combustion_model_message tcmm
    #         ORDER BY ts DESC
    #         LIMIT {l1},{l2}"""
    df = pd.read_sql(q, con)
    df_dict = df.astype(str).to_dict('records')
    return df_dict

def get_alarm_history_detail(alarmId):
    q = f"""SELECT
                f_actual_value AS actualValue,
                f_int_id AS alarmId,
                f_timestamp AS `date`,
                f_desc AS `desc`,
                f_set_value AS setValue
            FROM
                tb_combustion_alarm_history
            WHERE
                f_int_id = {alarmId}"""
    df = pd.read_sql(q, con)
    df_dict = df.astype(str).to_dict('records')[0]
    return df_dict

# def get_export_alarm_history(start, end):
#     start += ' 00:00:00'
#     end += ' 23:59:59'
#     q = f"""SELECT
#                 f_int_id AS alarmId,
#                 f_timestamp AS `date`,
#                 f_desc AS `desc`,
#                 f_set_value AS setValue,
#                 f_actual_value AS actualValue
#             FROM
#                 tb_combustion_alarm_history
#             WHERE
#                 f_timestamp BETWEEN '{start}' AND '{end}'"""
#
#     df = pd.read_sql(q, con)
#     df_dict = df.astype(str).to_dict('records')
#
#     if len(df_dict) > 0:
#         keys = df_dict[0].keys()
#         yield ','.join(keys) + '\n'
#
#         for row in df_dict:
#             yield ','.join(row.values()) + '\n'
#     else:
#         yield 'alarmId,date,desc,setValue,actualValue'

def get_export_alarm_history(start, end):
    start += ' 00:00:00'
    end += ' 23:59:59'
    q = f"""SELECT
                f_int_id AS alarmId,
                f_timestamp AS `date`,
                f_desc AS `desc`,
                f_set_value AS setValue,
                f_actual_value AS actualValue
            FROM
                tb_combustion_alarm_history
            WHERE
                f_timestamp BETWEEN '{start}' AND '{end}'"""
    
    df = pd.read_sql(q, con)
    df_dict = df.astype(str).to_dict('records')
    master = []
    
    if len(df_dict) > 0:
        keys = df_dict[0].keys()
        master.append(list(keys))
        
        for row in df_dict:
            master.append(list(row.values()))
    else:
        master.append(['alarmId','date','desc','setValue','actualValue'])
    
    return master

def get_rules_detailed(rule_id):
    q = f"""SELECT
                f_rule_dtl_id AS ruleDetailId,
                f_rule_hdr_id AS ruleHeaderId,
                f_sequence AS `sequence`,
                f_bracket_open AS bracketOpen,
                f_bracket_close AS bracketClose,
                f_tag_sensor AS tagSensor,
                f_violated_count AS violatedCount,
                f_max_violated AS maxViolated
            FROM
                {_DB_NAME_}.tb_combustion_rules_dtl tcrd
            WHERE
                f_rule_hdr_id = {rule_id}
            ORDER BY
                f_sequence"""
    df = pd.read_sql(q, con)
    df['maxViolated'] = df['maxViolated'].fillna(2)
    df_dict = df.to_dict('records')
    ret = {
        'detailRule': df_dict
    }
    return ret

# def get_rules_detailed(rule_id):
#     q = f"""SELECT f_rule_dtl_id AS ruleDetailId, f_rule_hdr_id AS ruleHeaderId, f_sequence AS sequence, f_bracket_open AS bracketOpen, f_bracket_close AS bracketClose, f_tag_sensor AS tagSensor 
#             FROM {_DB_NAME_}.tb_combustion_rules_dtl
#             WHERE f_rule_hdr_id = {rule_id} """
#     df = pd.read_sql(q, con)
#     df_dict = df.to_dict('records')
#     ret = {
#         'detailRule': df_dict
#     }
#     return ret

def get_tags_rule():
    q = f"""SELECT "" AS tagKKS, f_tag_name AS tagSensor, f_description AS tagDescription FROM {_DB_NAME_}.tb_tags_read_conf ttrc 
            WHERE f_category IN ('COPT', 'SOPT+COPT', 'COPT+SOPT', 'RULE SOPT')
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
            {_DB_NAME_}.tb_combustion_rules_dtl (f_rule_hdr_id, f_rule_descr, f_tag_sensor, f_rules, 
            f_operator, f_unit, f_limit_high, f_limit_low, f_sequence, f_bracket_open, 
            f_bracket_close, f_violated_count, f_max_violated, f_is_active, f_updated_at)
            VALUES """

    Payload = payload['detailRule']
    evaluate = ''
    tags_used = []
    for P in Payload:
        bracketOpen = P['bracketOpen']
        bracketClose = P['bracketClose']
        sequence = P['sequence']
        violatedCount = P['violatedCount']
        maxViolated = P['maxViolated']
        P['tagSensor'] = P['tagSensor'].replace(' ', '')
        tagSensor = [P['tagSensor'][:-1].split('(')[0] if '(' in P['tagSensor'] else P['tagSensor']][0]
        if 'ruleHeaderId' in P.keys(): ruleHeaderId = P['ruleHeaderId']
        else: ruleHeaderId = 20

        r = f"""( {ruleHeaderId} , NULL, '{tagSensor}', NULL, NULL, NULL, NULL, NULL, {sequence}, '{bracketOpen}', '{bracketClose}', '{violatedCount}', '{maxViolated}', 1, NOW()),\n"""
        q += r
        evaluate += f"{bracketOpen}{tagSensor}{bracketClose} "
        tags_used.append(tagSensor)
        
    q = q[:-2]

    # Value check
    qcheck = f"SELECT f_address_no, f_value FROM {_DB_NAME_}.tb_bat_raw WHERE f_address_no IN {tuple(tags_used)}"
    df = pd.read_sql(qcheck, con)
    df = df.set_index('f_address_no')['f_value']

    for k in df.index: evaluate = evaluate.replace(k, str(df[k]))

    try:
        Safeguard_status = eval(evaluate.lower().replace('=', '==').replace('TRUE','True').replace('true','True').replace('FALSE','False').replace('false','False'))
        qdel = f"""DELETE FROM {_DB_NAME_}.tb_combustion_rules_dtl
                   WHERE f_rule_hdr_id={ruleHeaderId}"""
        
        # engine = sqlalchemy.create_engine(con)

        # with engine.connect() as conn:
        with con.connect() as conn:
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

    # engine = sqlalchemy.create_engine(con)

    # with engine.connect() as conn:
    with con.connect() as conn:
        red = conn.execute(qdel)
        res = conn.execute(q)

    return {'Status':'Success'}

def update_alarm_desc(payload):
    alarmId = payload['alarmId']
    desc = payload['desc']

    q = f"""UPDATE
                tb_combustion_alarm_history
            SET
                f_desc = '{desc}'
            WHERE
                f_int_id = {alarmId}"""

    # engine = sqlalchemy.create_engine(con)

    # with engine.connect() as conn:
    with con.connect() as conn:
        conn.execute(q)

    return {'Status':'Success'}





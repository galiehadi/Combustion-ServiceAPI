# from operator import index
import pandas as pd
# import numpy as np
import time, db_config, sqlalchemy, requests
from urllib.parse import quote_plus as urlparse
# from pprint import pprint
from datetime import datetime
from asyncio.tasks import sleep
from random import random
# import threading

_UNIT_CODE_ = db_config._UNIT_CODE_
_UNIT_NAME_ = db_config._UNIT_NAME_
_USER_ = db_config._USER_
_PASS_ = urlparse(db_config._PASS_)
_IP_ = db_config._IP_
_DB_NAME_ = db_config._DB_NAME_
_LOCAL_IP_ = db_config._LOCAL_IP_


# LOCAL_MODE = True
# if LOCAL_MODE:
#     _IP_ = 'localhost:3308'
#     _LOCAL_IP_ = 'localhost'

# Default values
DEBUG_MODE = True
dcs_x = [0, 150, 255, 300, 330]
dcs_y = [8, 6.0, 4.5, 4.0, 4.0]

con = f"mysql+mysqlconnector://{_USER_}:{_PASS_}@{_IP_}/{_DB_NAME_}"
engine = sqlalchemy.create_engine(con)
perm_bias_alarm = {}

def logging(text):
    t = time.strftime('%Y-%m-%d %X')
    print(f"[{t}] - {text}")

def bg_update_permissive_bias():
    tag_perm_bias = get_constrain(db_config.COPT_PERM_BIAS)
    if bg_is_permissive_bias_in_range():
        write_to_opc(tag_perm_bias, 1)
        return True
    
    return False

def bg_is_permissive_bias_in_range():
    global perm_bias_alarm
    tags = get_constrains([db_config.COPT_OXY_LEFT, db_config.COPT_OXY_RIGHT])
    q = f"""SELECT
            ttrc.f_description, 
            tbr.f_address_no,
            tbr.f_value
        FROM
            tb_bat_raw tbr
        JOIN
        tb_tags_read_conf ttrc ON
            ttrc.f_tag_name = tbr.f_address_no
        WHERE
            tbr.f_address_no IN {tuple(tags)}"""
    df = pd.read_sql(q, con)

    bias_min = float(get_parameter_value(db_config.COPT_BIAS_MIN))
    bias_max = float(get_parameter_value(db_config.COPT_BIAS_MAX))
    perm_bias_alarm = {}
    is_in_range = True
    
    for i in df.index:
        tag_desc, tag_name, tag_value = df.iloc[i]
        tag_value = round(float(tag_value),2)

        if not (tag_value >= bias_min and tag_value <= bias_max):
            is_in_range = False
            perm_bias_alarm.update({tag_desc: str(tag_value) + '%'})
    
    return is_in_range;

# def bg_is_permissive_bias_in_range():
#     global perm_bias_alarm
#     tags = get_constrains([db_config.COPT_OXY_LEFT, db_config.COPT_OXY_RIGHT])
#     q = f"""SELECT
#             ttrc.f_description, 
#             tbr.f_address_no,
#             tbr.f_value
#         FROM
#             tb_bat_raw tbr
#         JOIN
#         tb_tags_read_conf ttrc ON
#             ttrc.f_tag_name = tbr.f_address_no
#         WHERE
#             tbr.f_address_no IN {tuple(tags)}"""
#     df = pd.read_sql(q, con)
#
#     bias_min = float(get_parameter_value(db_config.COPT_BIAS_MIN))
#     bias_max = float(get_parameter_value(db_config.COPT_BIAS_MAX))
#     perm_bias_alarm = {}
#
#     copt_run = get_tag_value(get_constrain(db_config.COPT_LAMP_INDICATOR))
#
#     if not copt_run:
#         return False
#
#     for i in df.index:
#         tag_desc, tag_name, tag_value = df.iloc[i]
#         tag_value = round(float(tag_value),2)
#         perm_bias_alarm.update({tag_desc:tag_value})
#
#         if tag_value >= bias_min and tag_value <= bias_max:
#             return True
#
#     return False

def copt_ok_check():
    global g_already_write_copt_disable_alarm
    if is_copt_ok():
        g_already_write_copt_disable_alarm = 0
        param_skip_fan_auto = get_copt_parameter(db_config.SKIP_FAN_AUTO)
        
        if param_skip_fan_auto == 1:
            is_enable = True
        else:
            is_enable = is_fan_in_auto_mode(is_copt_ok)
        enable_copt_run(is_enable)
        update_copt_last_running()
    

def get_copt_parameter(param_name):
    q = f"""SELECT
                f_default_value
            FROM
                tb_combustion_parameters tcp
            WHERE
                tcp.f_label = '{param_name}'"""
    df = pd.read_sql(q, con)
    
    return df['f_default_value'].loc[0]

def get_parameter_value(param_id):
    q = f"""SELECT
                f_default_value
            FROM
                tb_combustion_parameters tcp
            WHERE
                tcp.f_parameter_id = '{param_id}'"""
    df = pd.read_sql(q, con)
    
    return df['f_default_value'].loc[0]

def get_constrain(id):
    q = f"""SELECT
                f_constraint 
            FROM
                tb_comb_constraint tcc
            WHERE
                tcc.f_int_id = {id}"""
    df = pd.read_sql(q, con)
    
    return df['f_constraint'].loc[0]

def get_constrains(ids):
    q = f"""SELECT
                f_constraint 
            FROM
                tb_comb_constraint tcc
            WHERE
                tcc.f_int_id IN {tuple(ids)}"""
    df = pd.read_sql(q, con)
    return list(df['f_constraint'])

def get_constrain_by_value(value):
    q = f"""SELECT
                f_constraint,
                f_value
            FROM
                tb_comb_constraint tcc
            WHERE
                tcc.f_value LIKE '%{value}%' LIMIT 1"""
    df = pd.read_sql(q, con)
    
    return df['f_constraint'].loc[0], df['f_value'].loc[0]

def update_copt_last_running():
    tag_ok = get_constrain(db_config.COPT_OK)
    tag_on = get_constrain(db_config.COPT_ON)
    q = f"""SELECT
                f_value
            FROM
                tb_bat_raw_history tbrh
            WHERE
                tbrh.f_address_no = '{tag_ok}'
            ORDER BY
                tbrh.f_date_rec DESC
            LIMIT 1"""
    df = pd.read_sql(q, con)
    last_copt_ok = df['f_value'].loc[0]
    
    if last_copt_ok == 'False':
        q = f"""INSERT
                    INTO
                    tb_comb_constraint (f_int_id,
                    f_constraint,
                    f_value,
                    f_is_active,
                    f_last_update)
                VALUES ({db_config.CONSTRAIN_ID_COPT_ON},
                '{tag_on}',
                'True',
                1,
                NOW())
                            ON
                DUPLICATE KEY
                UPDATE
                    f_last_update = NOW(),
                    f_value = 'True'"""
                    
        with engine.connect() as conn:
            res = conn.execute(q)
        
def update_comb_constrain(id, name, last_update, value=1):
    q = f"""INSERT
                INTO
                tb_comb_constraint (f_int_id,
                f_constraint,
                f_value,
                f_is_active,
                f_last_update)
            VALUES ({id},
            '{name}',
            '{value}',
            1,
            '{last_update}')
                        ON
            DUPLICATE KEY
            UPDATE
                f_last_update = '{last_update}',
                f_value = '{value}'"""
                
    with engine.connect() as conn:
        res = conn.execute(q)
 
def enable_copt_run(is_enable):
    tag_run = get_constrain(db_config.COPT_RUN)
    write_to_opc(tag_run, is_enable)
    
# def enable_copt_run(is_enable):
#     tag_run = get_constrain(db_config.COPT_RUN)
#     copt_run = get_tag_value(tag_run)
#
#     if is_enable != copt_run:
#         write_to_opc(tag_run, is_enable)
    
def is_copt_ok():
    tag_ok = get_constrain(db_config.COPT_OK)
    return get_tag_value(tag_ok)

def bg_is_copt_enable():
    tags = get_constrains([db_config.COPT_ON, db_config.SAFEGUARD_DCS])
    q = f"""SELECT
            f_address_no,
            f_value
        FROM
            tb_bat_raw tbr
        WHERE
            tbr.f_address_no IN {tuple(tags)}"""
    df = pd.read_sql(q, con)
    copt_dict = {}
    
    for i in df.index:
        tag_name, tag_value = df.iloc[i]
        copt_dict.update({tag_name: tag_value})
    
    copt_list = list(copt_dict.values())
    
    if copt_list[0] and copt_list[1]:
        return True
    else:
        return False
    
def get_copt_enable_status():
    tag_enable = get_constrain(db_config.COPT_ENABLE)
    q = f"""SELECT
            f_value
        FROM
            tb_bat_raw tbr
        WHERE
            tbr.f_address_no = '{tag_enable}'"""
    df = pd.read_sql(q, con)
    
    return True if df['f_value'].loc[0] == 'True' else False

def bg_set_copt_enable(is_enable):
    tag_enable = get_constrain(db_config.COPT_ENABLE)
    q = f"""INSERT INTO {_DB_NAME_}.tb_bat_raw (f_address_no, f_date_rec, f_value, f_data_type, f_updated_at) VALUES ('{tag_enable}', NOW(), '{is_enable}', 'Boolean', NOW())
            ON DUPLICATE KEY UPDATE f_date_rec = NOW(), f_value = '{is_enable}', f_updated_at = NOW()"""
            
    with engine.connect() as conn:
        res = conn.execute(q)
        
def write_to_bat_raw(tag_name, tag_value, tag_type,):
    q = f"""INSERT INTO {_DB_NAME_}.tb_bat_raw (f_address_no, f_date_rec, f_value, f_data_type, f_updated_at) VALUES ('{tag_name}', NOW(), '{tag_value}', '{tag_type}', NOW())
            ON DUPLICATE KEY UPDATE f_date_rec = NOW(), f_value = '{tag_value}', f_updated_at = NOW()"""
            
    with engine.connect() as conn:
        res = conn.execute(q)
        
def is_copt_on():
    tag_on = get_constrain(db_config.COPT_ON)
    q = f"""SELECT
            tbr.f_value
        FROM
            tb_bat_raw tbr
        WHERE
            tbr.f_address_no = '{tag_on}'"""
    df = pd.read_sql(q, con)
    
    return True if df['f_value'].loc[0] == 'True' else False
        
# def bg_helper_update_copt_run():
#     logging(f"Checking AUTO mode in SA Fan, PA Fan, ID Fan and PA Booster")
#     for i in range(10):
#         is_auto_mode = is_fan_in_auto_mode()
#         if is_auto_mode:
#             logging(f"Enabling {db_config.COPT_RUN}")
#             write_to_opc(db_config.COPT_RUN, 1)
#             return True
#
#         if i == 9:
#             logging(f"Not in AUTO mode, Failed to enabling {db_config.COPT_RUN}")
#             return False
#
#         time.sleep(1)
        
def test_write_tag(tag_name, value):
    write_to_opc(tag_name, value)

def read_tag(tag):
    q = f"""SELECT
            tbr.f_address_no, f_value
        FROM
            tb_bat_raw tbr
        WHERE
            tbr.f_address_no = '{tag}'"""
    df = pd.read_sql(q, con)
    
    logging(f"RESPONSE:")
    logging(df)
    
def get_tag_value(tag):
    q = f"""SELECT
            tbr.f_value
        FROM
            tb_bat_raw tbr
        WHERE
            tbr.f_address_no = '{tag}'"""
    df = pd.read_sql(q, con)
    
    raw_value = df['f_value'].loc[0]
    
    if raw_value == 'True':
        return True
    if raw_value == 'False':
        return False
    
    return raw_value    

def write_to_opc(tag_name, value):
    try:
        opc_write = [[tag_name, datetime.now(), value]]
        opc_write = pd.DataFrame(opc_write, columns=['f_tag','f_timestamp','f_value'])
        opc_write.to_sql('tb_opc_write', con, if_exists='append', index=False)
        opc_write.to_sql('tb_opc_write_history', con, if_exists='append', index=False)
    except Exception as e:
        logging(e)
        
def write_to_opc_exclude_history(tag_name, value):
    try:
        opc_write = [[tag_name, datetime.now(), value]]
        opc_write = pd.DataFrame(opc_write, columns=['f_tag','f_timestamp','f_value'])
        opc_write.to_sql('tb_opc_write', con, if_exists='append', index=False)
        # opc_write.to_sql('tb_opc_write_history', con, if_exists='append', index=False)
    except Exception as e:
        logging(e)
        

def is_fan_in_auto_mode(copt_ok):
    # memastikan parameter di bawah terpenuhi (True)
    # tags = get_constrains([db_config.COPT_SA_FAN, db_config.COPT_PA_FAN, db_config.COPT_PA_BOOSTER, db_config.COPT_ID_FAN])
    tags = get_constrains([db_config.COPT_SA_FAN, db_config.COPT_PA_FAN, db_config.COPT_ID_FAN])
    q = f"""SELECT
            ttrc.f_description, tbr.f_value
        FROM
            tb_bat_raw tbr
        JOIN tb_tags_read_conf ttrc ON
            ttrc.f_tag_name = tbr.f_address_no
        WHERE
            tbr.f_address_no IN {tuple(tags)}"""
    df = pd.read_sql(q, con)
    copt_dict = {}
    
    for i in df.index:
        tag_name, tag_value = df.iloc[i]
        copt_dict.update({tag_name: True if tag_value == 'True' else False})
    
    copt_list = list(copt_dict.values())
    
    # if copt_list[0] or copt_list[1] or copt_list[2] or copt_list[3]:
    if copt_list[0] and copt_list[1] and copt_list[2]:
        return True
    else:
        if copt_ok:
            copt_dict = {key:val for key, val in copt_dict.items() if val == False}
            actual_value = str(copt_dict).replace('{', '').replace('}', '').replace('True','Auto').replace('False', 'Manual').replace('\'','')
            write_alarm(actual_value, 'COPT ENABLE FAIL: FAN NOT IN AUTO MODE', 0)
        return False

def bg_safeguard_check():
    t0 = time.time()
    q = f"""
        SELECT
            rule.f_rule_dtl_id,
            rule.f_tag_sensor,
            conf.f_description,
            rule.f_sequence,
            rule.f_bracket_open,
            raw.f_value,
            rule.f_bracket_close,
            rule.f_violated_count,
            rule.f_max_violated
        FROM
            {_DB_NAME_}.tb_combustion_rules_dtl rule
        LEFT JOIN {_DB_NAME_}.tb_bat_raw raw ON
            rule.f_tag_sensor = raw.f_address_no
        LEFT JOIN {_DB_NAME_}.tb_combustion_rules_hdr hdr ON
            rule.f_rule_hdr_id = hdr.f_rule_hdr_id
        LEFT JOIN {_DB_NAME_}.tb_tags_read_conf conf ON 
            rule.f_tag_sensor = conf.f_tag_name 
        WHERE
            hdr.f_rule_hdr_id = {db_config.RULE_SAFEGUARD}
        ORDER BY
            rule.f_sequence"""
    df = pd.read_sql(q, con)
    df['f_max_violated'] = df['f_max_violated'].fillna(2)

    Safeguard_status = True
    Safeguard_text = ''

    individual_results = []
    for i in df.index:
        description, bracketOpen, value, bracketClose = df[['f_description','f_bracket_open','f_value','f_bracket_close']].iloc[i]
        Safeguard_text += f"{bracketOpen}{value}{bracketClose} "

        safeguard_text = f"{bracketOpen} {df.loc[i, 'f_value']} {bracketClose}"
        safeguard_text = safeguard_text.lower().replace('and','').replace('or','').replace('false','False').replace('=','==')
        safeguard_text = safeguard_text.replace('====','==').replace('(','').replace(')','')

        safeguard_result = eval(safeguard_text)
        individual_results.append(safeguard_result)

    Safeguard_text = Safeguard_text.lower().replace('=', '==').replace('TRUE','True').replace('true','True').replace('FALSE','False').replace('false','False')
    Safeguard_status = eval(Safeguard_text)

    df['individual_safeguard'] = individual_results
    df['f_violated_count'] = [vc+1 if not sr else 0 for vc,sr in df[['f_violated_count', 'individual_safeguard']].values]
    df['safeguard_violated'] = df['f_violated_count'] < df['f_max_violated']
    all_safeguard = df['safeguard_violated'].min()

    # Update safeguard counter
    with engine.connect() as conn:
        send = df[['f_rule_dtl_id','f_violated_count']]
        for i in send.index:
            q = f"""UPDATE tb_combustion_rules_dtl
                    SET f_violated_count = {int(send.loc[i, 'f_violated_count'])}
                    WHERE f_rule_dtl_id = {int(send.loc[i, 'f_rule_dtl_id'])} """
            conn.execute(q)

    RetObject = df[['f_sequence','f_description','f_value','f_bracket_close','individual_safeguard']]
    RetObject = RetObject.rename(columns = {
        'f_sequence':'sequence',
        'f_description': 'tagDescription',
        'f_value': 'actualValue',
        'f_bracket_close': 'setValue',
        'individual_safeguard': 'status'
    })

    # Bypass safeguard status with safeguard counter
    Safeguard_status = bool(all_safeguard)

    if not Safeguard_status:
        retprint = RetObject[RetObject['status'] == False][['tagDescription','actualValue','setValue']].values
        retprint = '\n'.join([' '.join(f) for f in retprint])
        logging(f"SAFEGUARD not in Safe conditions:\n{retprint}")

    ret = {
        'Safeguard Status': Safeguard_status,
        'Execution time': str(round(time.time() - t0,3)) + ' sec',
        'detailRule': RetObject.to_dict(orient='records'),
        'label': 'Safeguard',
        'ruleLogic': Safeguard_text,
        'ruleValue': Safeguard_status,
    }
    return ret

def is_safeguard_safe():
    tag_safeguard = get_constrain(db_config.SAFEGUARD_BAT)
    q = f"""SELECT
            tbr.f_value
        FROM
            tb_bat_raw tbr
        WHERE
            tbr.f_address_no = '{tag_safeguard}'"""
    df = pd.read_sql(q, con)
    
    return True if df['f_value'].loc[0] == 'True' else False

def bg_safeguard_update_runner():
    while True:
        bg_safeguard_update()
        time.sleep(db_config.COPT_BG_SLEEP_TIME)

def get_latest_safeguard():
    tag_safeguard = get_constrain(db_config.SAFEGUARD_DCS)
    q = f"""SELECT
                f_value
            FROM
                tb_bat_raw_history tbrh
            WHERE
                tbrh.f_date_rec > NOW() - INTERVAL 1 HOUR
                AND tbrh.f_address_no = '{tag_safeguard}'
            ORDER BY
                tbrh.f_date_rec DESC
            LIMIT 1"""
    df = pd.read_sql(q, con)
    
    return True if df['f_value'].loc[0] == 'True' else False
    
def bg_safeguard_update():
    ret = bg_safeguard_check()
    Safeguard_status = ret['Safeguard Status']

    # Update SAFEGUARD WEB INDICATOR (1 if Safe, 0 if Not Safe)
    tag_safeguard = get_constrain(db_config.SAFEGUARD_BAT)
    q = f"""INSERT INTO {_DB_NAME_}.tb_bat_raw (f_address_no, f_date_rec, f_value, f_data_type, f_updated_at) VALUES ('{tag_safeguard}', NOW(), '{Safeguard_status}', 'Boolean', NOW())
            ON DUPLICATE KEY UPDATE f_date_rec = NOW(), f_value = '{Safeguard_status}', f_updated_at = NOW()"""
    with engine.connect() as conn:
        res = conn.execute(q)
        
    # Update SAFEGUARD DCS INDICATOR (1 if Not Safe, 0 is Safe)
    tag_safeguard = get_constrain(db_config.SAFEGUARD_DCS)
    opc_write = [[tag_safeguard, datetime.now(), 0 if Safeguard_status else 1]]
    opc_write = pd.DataFrame(opc_write, columns=['f_tag','f_timestamp','f_value'])
    opc_write.to_sql('tb_opc_write', con, if_exists='append', index=False)
    opc_write.to_sql('tb_opc_write_history', con, if_exists='append', index=False)

    # Update Tag Enable COPT to False if 
    if not ret['Safeguard Status']:
        tag_enable = get_constrain(db_config.COPT_ENABLE)
        q = f"""INSERT INTO  {_DB_NAME_}.tb_bat_raw (f_address_no, f_date_rec, f_value, f_data_type, f_updated_at) VALUES ('{tag_enable}', NOW(), 'False', 'Boolean', NOW())
            ON DUPLICATE KEY UPDATE f_date_rec = NOW(), f_value = 'False', f_updated_at = NOW()"""
        with engine.connect() as conn:
            res = conn.execute(q)
    
    return ret

def bg_get_recom_exec_interval():
    q = f"""SELECT f_default_value FROM {_DB_NAME_}.tb_combustion_parameters tcp 
            WHERE f_label = 'RECOM_EXEC_INTERVAL' """
    df = pd.read_sql(q, con)
    recom_exec_interval = float(df.values)
    return recom_exec_interval

def bg_get_ml_recommendation():
    try:
        response = requests.get(f'http://{_LOCAL_IP_}:5002/bat_combustion/{_UNIT_CODE_}/realtime')
        ret = response.json()
        return ret
    except Exception as e:
        logging(f"{time.ctime()} - Machine learning prediction error: {e}")
        return str(e)

def bg_model_runner():
    while True:
        bg_ml_runner()
        time.sleep(db_config.COPT_BG_SLEEP_TIME)

# def get_last_suggestions():
#     oxy_desc = db_config.COPT_OXY_DESC
#     pres_desc = db_config.COPT_PRES_DESC
#
#     q = f"""SELECT
#                 tcmg.tag_name, tcmg.value, tcmg.ts
#             FROM
#                 tb_combustion_model_generation tcmg
#             WHERE
#                 tcmg.tag_name = 'SA Heater Out Press' OR tcmg.tag_name LIKE 'Excess Oxygen%'
#                 AND tcmg.ts >= (
#                 SELECT
#                     f_last_update
#                 FROM
#                     tb_comb_constraint tcc
#                 WHERE
#                     tcc.f_constraint = '{db_config.COPT_ON}')
#             ORDER BY
#                 tcmg.ts DESC, tcmg.tag_name DESC
#             LIMIT 2"""
#     df = pd.read_sql(q, con)
#     copt_dict = {}
#
#     for i in df.index:
#         tag_name, tag_value, = df.iloc[i]
#
#         if (oxy_desc in tag_name):
#             tag_name = db_config.COPT_OXY_IN
#
#         if (tag_name == pres_desc):
#             tag_name = db_config.COPT_PRES_IN
#
#         copt_dict.update({tag_name: tag_value})
#
#     return copt_dict

def get_last_suggestions():
    # oxy_desc = db_config.COPT_OXY_DESC
    # pres_desc = db_config.COPT_PRES_DESC
    recom_oxy = get_constrain(db_config.RECOM_OXY)
    recom_press = get_constrain(db_config.RECOM_PRESS)
    recom_coal = get_constrain(db_config.RECOM_COAL)
    q = f"""SELECT
                tcmg.tag_name, tcmg.value, tcmg.ts
            FROM
                tb_combustion_model_generation tcmg
            WHERE
                tcmg.tag_name IN {(recom_oxy,recom_press)}
                AND tcmg.ts >= (
                SELECT
                    f_last_update
                FROM
                    tb_comb_constraint tcc
                WHERE
                    tcc.f_int_id = '{db_config.COPT_ON}')
            ORDER BY
                tcmg.ts DESC, tcmg.tag_name DESC LIMIT 2"""
    
    df = pd.read_sql(q, con)
    suggestions = []
    tag_oxy = get_constrain(db_config.COPT_OXY_IN)
    tag_press = get_constrain(db_config.COPT_PRES_IN)
    tag_coal = get_constrain(db_config.COPT_COAL_IN)
    
    for i in df.index:
        tag_name, tag_value, ts = df.iloc[i]
        if tag_name == recom_oxy:
            tag_name = tag_oxy
            
        if tag_name == recom_press:
            tag_name = tag_press
            
        if tag_name == recom_coal:
            tag_name = tag_coal
        
        suggestions.append({'tag':tag_name, 'value':tag_value, 'ts':ts})
    
    return suggestions

def is_copt_lamp():
    tag_lamp = get_constrain(db_config.COPT_LAMP_INDICATOR)
    q = f"""SELECT
            tbr.f_value
        FROM
            tb_bat_raw tbr
        WHERE
            tbr.f_address_no = '{tag_lamp}'"""
    df = pd.read_sql(q, con)
    
    return True if df['f_value'].loc[0] == 'True' else False

def is_permissive_bias():
    tag_perm_bias = get_constrain(db_config.COPT_PERM_BIAS)
    return get_tag_value(tag_perm_bias)

# def update_permissive_bias():
#     tag_perm_bias = get_constrain(db_config.COPT_PERM_BIAS)
#     if is_copt_lamp():
#         is_in_range = bg_is_permissive_bias_in_range()
#         write_to_opc(tag_perm_bias, is_in_range)
#         write_to_bat_raw(tag_perm_bias, is_in_range, 'Boolean')
#     else:
#         perm_bias = get_tag_value(tag_perm_bias)
#         write_to_bat_raw(tag_perm_bias, 'False', 'Boolean')
#         write_to_opc(tag_perm_bias, 'False')

def update_permissive_bias():
    tag_perm_bias = get_constrain(db_config.COPT_PERM_BIAS)
    is_in_range = bg_is_permissive_bias_in_range()
    copt_run = get_tag_value(get_constrain(db_config.COPT_LAMP_INDICATOR))
    
    if not copt_run:
        is_in_range = False
    
    write_to_opc(tag_perm_bias, is_in_range)
    write_to_bat_raw(tag_perm_bias, is_in_range, 'Boolean')

def is_dcs_copt_enable_lamp():
    return get_tag_value(get_constrain(db_config.COPT_LAMP_INDICATOR))

def get_ml_last_run_time():
    q = f"SELECT f_last_update FROM tb_comb_constraint tcc WHERE tcc.f_int_id = {db_config.CONSTRAIN_ID_ML_LAST_RUN}"
    df = pd.read_sql(q, con)
    
    return df['f_last_update'].loc[0]

def get_safeguard_detail():
    q = f"""SELECT
                rule.f_tag_sensor,
                conf.f_description,
                rule.f_bracket_open,
                raw.f_value,
                rule.f_bracket_close
            FROM
                {_DB_NAME_}.tb_combustion_rules_dtl rule
            LEFT JOIN {_DB_NAME_}.tb_bat_raw raw ON
                rule.f_tag_sensor = raw.f_address_no
            LEFT JOIN {_DB_NAME_}.tb_combustion_rules_hdr hdr ON
                rule.f_rule_hdr_id = hdr.f_rule_hdr_id
            LEFT JOIN {_DB_NAME_}.tb_tags_read_conf conf ON 
                rule.f_tag_sensor = conf.f_tag_name 
            WHERE
                hdr.f_rule_hdr_id = {db_config.RULE_SAFEGUARD}
            ORDER BY
                rule.f_sequence"""
    df = pd.read_sql(q, con)
    sg = df[['f_description','f_value','f_bracket_close','f_tag_sensor']]
    alarm_desc = ''
    alarm_set_value = ''
    alarm_actual_value = ''
    eval_string = ''
    dcs_err = {}
    
    for i in sg.index:
        description, raw_value, bracketClose, tag_name = sg.iloc[i]
        try:
            raw_value = round(float(raw_value),2)
        except Exception as e:
            print(e)
        set_value = f"{bracketClose.lower().replace(' ','').replace(')','').replace('and','').replace('or','').replace('true','True').replace('false','False').replace('=','==')}"
        rule = f"{raw_value}{bracketClose.lower().replace(' ','').replace(')','').replace('and','').replace('or','').replace('true','True').replace('false','False').replace('=','==')}"
        eval_result = eval(rule)
        
        constraint, value = get_constrain_by_value(tag_name)
        if 'or' in value or 'and' in value:
            if eval_string == eval_string.replace(tag_name, str(eval_result)):
                eval_string = ''
                
            if eval_string == '':
                eval_string = value
            
            eval_string = eval_string.replace(tag_name, str(eval_result))
            
            try:
                dcs_eval = eval(eval_string)
                dcs_err.update({constraint: dcs_eval})
            except Exception as e:
                # do nothing
                print('do nothing')
        else:
            dcs_err.update({constraint: eval_result})
        
        if not eval_result:
            alarm_desc = f"{alarm_desc}{description}[{rule}] "
            alarm_actual_value = f"{alarm_actual_value}{description} [{raw_value}] "
            alarm_set_value = f"{alarm_set_value}{description} [{set_value}] "
    
    for i in dcs_err:
        write_to_opc(i, dcs_err[i])
        
    return alarm_desc, alarm_set_value, alarm_actual_value

def write_alarm(actual_value, desc, header, set_value=''):
    try:
        actual_value = round(float(actual_value),2)
    except Exception as e:
        print(e)
    
    q = f"""INSERT 
                INTO tb_combustion_alarm_history (f_timestamp, f_actual_value, f_desc, f_rule_header, f_set_value) 
                VALUE (NOW(), '{actual_value}', '{desc}', {header}, '{set_value}')"""
    with engine.connect() as conn:
            res = conn.execute(q)

def is_safeguard():
    tag_safeguard = get_constrain(db_config.SAFEGUARD_BAT)
    return get_tag_value(tag_safeguard)

def write_alarm_safeguard():
    if not is_safeguard():
        alarm_desc, set_value, actual_value = get_safeguard_detail()
        
        if actual_value:
            write_alarm(actual_value, 'Safeguard: ' + alarm_desc, 20, set_value)

def bg_alarm_runner():
    # if is_copt_lamp():
    #     write_alarm_safeguard()
    # write_alarm_safeguard()    
    write_copt_disable_alarm()
    write_watchdog_alarm()
    write_alarm_safeguard_details()

g_already_write_copt_disable_alarm = 0
def write_copt_disable_alarm():
    global g_already_write_copt_disable_alarm
    copt_ok = get_constrain(db_config.COPT_OK)
    copt_ok_val = get_tag_value(copt_ok)
    
    if not copt_ok_val:
        if g_already_write_copt_disable_alarm == 0:
            write_alarm('', 'Status Disable COPT', 20, '')
            g_already_write_copt_disable_alarm = 1
    
g_already_write_watchdog_alarm = 0        
def write_watchdog_alarm():
    global g_already_write_watchdog_alarm
    watchdog = get_watchdog_indicator()
    
    if watchdog == '0' and g_already_write_watchdog_alarm == 0:
        write_alarm('False', "Status Watchdog Failed", 20, 'True')
        g_already_write_watchdog_alarm = 1
    if watchdog == '1':
        print('jangan masuk sini')
        g_already_write_watchdog_alarm = 0

def write_alarm_safeguard_details():
    q = f"""SELECT
                rule.f_tag_sensor,
                conf.f_description,
                rule.f_bracket_open,
                raw.f_value,
                rule.f_bracket_close
            FROM
                {_DB_NAME_}.tb_combustion_rules_dtl rule
            LEFT JOIN {_DB_NAME_}.tb_bat_raw raw ON
                rule.f_tag_sensor = raw.f_address_no
            LEFT JOIN {_DB_NAME_}.tb_combustion_rules_hdr hdr ON
                rule.f_rule_hdr_id = hdr.f_rule_hdr_id
            LEFT JOIN {_DB_NAME_}.tb_tags_read_conf conf ON 
                rule.f_tag_sensor = conf.f_tag_name 
            WHERE
                hdr.f_rule_hdr_id = {db_config.RULE_SAFEGUARD}
            ORDER BY
                rule.f_sequence"""
    df = pd.read_sql(q, con)
    sg = df[['f_description','f_value','f_bracket_close','f_tag_sensor']]
    
    for i in sg.index:
        description, raw_value, bracketClose, tag_name = sg.iloc[i]
        
            
        set_value = f"{bracketClose.lower().replace(' ','').replace(')','').replace('and','').replace('or','').replace('true','True').replace('false','False').replace('=','==')}"
        rule = f"{raw_value}{bracketClose.lower().replace(' ','').replace(')','').replace('and','').replace('or','').replace('true','True').replace('false','False').replace('=','==')}"
        eval_result = eval(rule)
        
        if not eval_result:
            if bracketClose.__contains__('>'):
                description += ' is LOW'
            elif bracketClose.__contains__('<'):
                description += ' is HIGH'
            else:
                description += ' is NOT OK' 
            write_alarm(raw_value, description, 20, f"{tag_name}{set_value}")
        else:
            write_alarm(raw_value, description + ' is OK', 20, f"{tag_name}{set_value}")

def get_watchdog_indicator():
    q = f"""SELECT
                f_default_value
            FROM
                tb_sootblow_control tsc
            WHERE
                tsc.f_control_id = {db_config.WATCHDOG_INDICATOR}"""
    df = pd.read_sql(q, con)
    return df['f_default_value'][0]    
        
def write_latest_o2_suggest():
    tag_write = get_constrain(db_config.CONSTRAIN_O2_SUGGEST)
    tag_read = get_constrain(db_config.RECOM_OXY)
    
    q = f"""SELECT
                tcog.value
            FROM
                tb_combustion_model_generation tcog
            WHERE
                tcog.tag_name = '{tag_read}'
            ORDER BY
                tcog.ts DESC
            LIMIT 1"""
    
    df = pd.read_sql(q, con)
    
    if df.shape[0] > 0:
        tag_value = df['value'].loc[0]
        write_to_bat_raw(tag_write, tag_value, 'Float')
        
def write_latest_press_suggest():
    tag_write = get_constrain(db_config.CONSTRAIN_PRESS_SUGGEST)
    tag_read = get_constrain(db_config.RECOM_PRESS)
    
    q = f"""SELECT
                tcog.value
            FROM
                tb_combustion_model_generation tcog
            WHERE
                tcog.tag_name = '{tag_read}'
            ORDER BY
                tcog.ts DESC
            LIMIT 1"""
    
    df = pd.read_sql(q, con)
    
    if df.shape[0] > 0:
        tag_value = df['value'].loc[0]
        write_to_bat_raw(tag_write, tag_value, 'Float')
        
def write_latest_coal_suggest():
    tag_write = get_constrain(db_config.CONSTRAIN_COAL_SUGGEST)
    tag_read = get_constrain(db_config.RECOM_COAL)
    
    q = f"""SELECT
                tcog.value
            FROM
                tb_combustion_model_generation tcog
            WHERE
                tcog.tag_name = '{tag_read}'
            ORDER BY
                tcog.ts DESC
            LIMIT 1"""
    
    df = pd.read_sql(q, con)
    
    if df.shape[0] > 0:
        tag_value = df['value'].loc[0]
        write_to_bat_raw(tag_write, tag_value, 'Float')

def bg_ml_runner():
    step = 1
    
    while True:
        if is_copt_lamp():
            if step == 1:
                step_call_suggestion()
                write_latest_o2_suggest()
                write_latest_press_suggest()
                write_latest_coal_suggest()
                step += 1
                continue
            if step == 2:
                step_write_suggestion()
                return
        else:
            debug_mode = get_parameter_value(db_config.PARAM_DEBUG_MODE)
            
            if debug_mode == 1:
                step_call_suggestion_in_debug_mode()
                write_latest_o2_suggest()
                write_latest_press_suggest()
                write_latest_coal_suggest()
                step_write_o2_suggestion()
           
            return

# def bg_ml_runner():
#     write_latest_o2_suggest()
#     step = 1
#
#     while True:
#         if is_copt_lamp():
#             if step == 1:
#                 step_call_suggestion()
#                 step += 1
#                 continue
#             if step == 2:
#                 step_write_suggestion()
#                 return
#         else:
#             debug_mode = get_parameter_value(db_config.PARAM_DEBUG_MODE)
#
#             if debug_mode == 1:
#                 step_call_suggestion_in_debug_mode() 
#
#             return

def get_inrange_smallest_oxy():
    tags = get_constrains([db_config.COPT_OXY_LEFT, db_config.COPT_OXY_RIGHT])
    q = f"""SELECT
            ttrc.f_description, 
            tbr.f_address_no,
            tbr.f_value
        FROM
            tb_bat_raw tbr
        JOIN
        tb_tags_read_conf ttrc ON
            ttrc.f_tag_name = tbr.f_address_no
        WHERE
            tbr.f_address_no IN {tuple(tags)}"""
    
    df = pd.read_sql(q, con)
    oxy_a = float(df['f_value'].loc[0])
    oxy_b = float(df['f_value'].loc[1])
    smallest_oxy = oxy_a
    
    if oxy_b < smallest_oxy:
        smallest_oxy = oxy_b
    
    oxy_min = float(get_parameter_value(db_config.COPT_OXY_MIN))
    oxy_max = float(get_parameter_value(db_config.COPT_OXY_MAX))
    
    if smallest_oxy >= oxy_min and smallest_oxy <= oxy_max:
        return smallest_oxy
    elif smallest_oxy > oxy_max:
        return oxy_max - random()
    else:
        return oxy_min

def write_smallest_pv_oxy():
    if is_permissive_bias():
        tag_oxy = get_constrain(db_config.COPT_OXY_PV_IN)
        smallest_oxy = get_inrange_smallest_oxy()
        write_to_opc(tag_oxy, smallest_oxy)

def step_write_suggestion():
    logging(f"Write latest recommendations to DCS")
    suggestions = get_last_suggestions()
    last_run = get_ml_last_run_time()
    
    for suggest in suggestions:
        tag = suggest.get('tag')
        value = suggest.get('value')
        ts = suggest.get('ts')
        
        if last_run != ts:
            if is_permissive_bias():
                logging(f"write suggestion {tag} {value}")
                write_to_opc(tag, value)
                update_comb_constrain(db_config.CONSTRAIN_ID_ML_LAST_RUN, 
                                      db_config.CONSTRAIN_DESC_ML_LAST_RUN, ts)
                
def step_write_o2_suggestion():
    logging(f"Write latest recommendations to DCS")
    suggestions = get_last_suggestions()
    # last_run = get_ml_last_run_time()
    for suggest in suggestions:
        tag = suggest.get('tag')
        value = suggest.get('value')
        
        if tag == 'O2_SV2':
            write_to_opc(tag, value)
            # ts = suggest.get('ts')
            
            # if last_run != ts:
                # logging(f"write suggestion {tag} {value}")
                # update_comb_constrain(db_config.CONSTRAIN_ID_ML_LAST_RUN, 
                #                       db_config.CONSTRAIN_DESC_ML_LAST_RUN, ts)
                    
        

# def step_write_suggestion():
#     logging(f"Write latest recommendations to DCS")
#     suggestions = get_last_suggestions()
#
#     if is_permissive_bias():
#         last_run = get_ml_last_run_time()
#
#         for suggest in suggestions:
#             tag = suggest.get('tag')
#             value = suggest.get('value')
#             ts = suggest.get('ts')
#
#             if last_run != ts:
#                 logging(f"write suggestion {tag} {value}")
#                 write_to_opc(tag, value)
#                 update_comb_constrain(96, db_config.CONSTRAIN_DESC_ML_LAST_RUN, ts)
            

def step_call_suggestion():
    if get_permissive_bias():
        print(f"Calling ML API for recommendations")
        bg_get_ml_recommendation()
    else:
        # write_alarm(str(perm_bias_alarm).replace('{','').replace('}','').replace('\'',''), 'COPT SUGGESTION FAIL: PERMISSIVE BIAS', 1)
        write_alarm(str(perm_bias_alarm).replace('{','').replace('}','').replace('\'',''), 'Status Oxygen Bias Range Suggestion Failed', 1, 'Oxygen Bias Range 2% - 20%')
        
def step_call_suggestion_in_debug_mode():
    if bg_is_permissive_bias_in_range():
        print(f"Calling ML API for recommendations (debug_mode)")
        bg_get_ml_recommendation()
    else:
        # write_alarm(str(perm_bias_alarm).replace('{','').replace('}','').replace('\'',''), 'COPT SUGGESTION FAIL: PERMISSIVE BIAS', 1)
        write_alarm(str(perm_bias_alarm).replace('{','').replace('}','').replace('\'',''), 'Status Oxygen Bias Range Suggestion Failed', 1, 'Oxygen Bias Range 2% - 20%')
    
def get_permissive_bias():
    tag_perm_bias = get_constrain(db_config.COPT_PERM_BIAS)
    return get_tag_value(tag_perm_bias)
            
g_x2_o2_l = False
g_x2_o2_r = False
def bg_safeguard_dcs_indicator():
    global g_x2_o2_l
    global g_x2_o2_r
    tags = get_constrains([db_config.COPT_OXY_LEFT, db_config.COPT_OXY_RIGHT])
    sg_oxy_l = get_constrain(db_config.SG_OXY_L)
    sg_oxy_r = get_constrain(db_config.SG_OXY_R)
    total_count_oxy_left = 0
    total_count_oxy_right = 0

    while True:
        counter_oxy_left = 0
        counter_oxy_right = 0
        time_start = time.time()

        while(time.time() - time_start < 3):
            query_rule_oxy_left = f"""
                SELECT 
                    tbr.f_value,
                    f_bracket_close
                FROM
                    tb_combustion_rules_dtl tcrd
                JOIN tb_bat_raw tbr ON
                    tbr.f_address_no = tcrd.f_tag_sensor
                WHERE
                    tcrd.f_rule_hdr_id = 20
                    AND tcrd.f_tag_sensor IN {tuple(tags)}
                ORDER BY
                    tcrd.f_tag_sensor ASC"""

            df = pd.read_sql_query(query_rule_oxy_left, engine)
            df_dict = df.astype(str).to_dict('records')
            oxy_rule_left = df_dict[0]['f_value'] + df_dict[0]['f_bracket_close']
            oxy_rule_left = oxy_rule_left.lower().replace(')','').replace('and','')
            eval_oxy_left = eval(oxy_rule_left)

            oxy_rule_right = df_dict[1]['f_value'] + df_dict[1]['f_bracket_close']
            oxy_rule_right = oxy_rule_right.lower().replace(')','').replace('and','')
            eval_oxy_right = eval(oxy_rule_right)

            if eval_oxy_left:
                total_count_oxy_left = 0
                counter_oxy_left = 0
                write_to_opc_exclude_history(sg_oxy_l, False)
                g_x2_o2_l = False
            else:
                counter_oxy_left += 1

            if eval_oxy_right:
                total_count_oxy_right = 0
                counter_oxy_right = 0
                write_to_opc_exclude_history(sg_oxy_r, False)
                g_x2_o2_r = False
            else:
                counter_oxy_right += 1
            time.sleep(0.5)

        if counter_oxy_left > 0:
            total_count_oxy_left += 1

        if counter_oxy_right > 0:
            total_count_oxy_right += 1

        if total_count_oxy_left >= 3:
            write_to_opc_exclude_history(sg_oxy_l, True)
            g_x2_o2_l = True

        if total_count_oxy_right >= 3:
            write_to_opc_exclude_history(sg_oxy_r, True)
            g_x2_o2_r = True

def bg_maintain_x2_o2_true():
    while True:
        if not is_safeguard():
            sg_oxy_l = get_constrain(db_config.SG_OXY_L)
            sg_oxy_r = get_constrain(db_config.SG_OXY_R)
            time.sleep(0.5)
            
            if g_x2_o2_l == True:
                x2_o2_l = get_tag_value(sg_oxy_l)
    
                if x2_o2_l == False:
                    write_to_opc(sg_oxy_l, True)
            
            if g_x2_o2_r == True:
                x2_o2_r = get_tag_value(sg_oxy_r)
    
                if x2_o2_r == False:
                    write_to_opc(sg_oxy_r, True)
        else:
            time.sleep(1)

def bg_maintain_x2_o2l_true():
    while True:
        if not is_safeguard():
            sg_oxy_l = get_constrain(db_config.SG_OXY_L)
            
            if g_x2_o2_l == True:
                time.sleep(0.75)
                x2_o2_l = get_tag_value(sg_oxy_l)
    
                if x2_o2_l == False:
                    write_to_opc(sg_oxy_l, True)
        else:
            time.sleep(1)

def bg_maintain_x2_o2r_true():
    while True:
        if not is_safeguard():
            sg_oxy_r = get_constrain(db_config.SG_OXY_R)
            
            if g_x2_o2_r == True:
                time.sleep(0.75)
                x2_o2_r = get_tag_value(sg_oxy_r)
    
                if x2_o2_r == False:
                    write_to_opc(sg_oxy_r, True)
        else:
            time.sleep(1)
            
# g_x2_o2_l = False
# g_x2_o2_r = False
# def bg_safeguard_dcs_indicator():
#     global g_x2_o2_l
#     global g_x2_o2_r
#     tags = get_constrains([db_config.COPT_OXY_LEFT, db_config.COPT_OXY_RIGHT])
#     sg_oxy_l = get_constrain(db_config.SG_OXY_L)
#     sg_oxy_r = get_constrain(db_config.SG_OXY_R)
#     total_count_oxy_left = 0
#     total_count_oxy_right = 0
#
#     while True:
#         param_skip_dcs_sg_indicator = get_copt_parameter(db_config.PARAM_SKIP_DCS_SG_INDICATOR)
#
#         if param_skip_dcs_sg_indicator == 1:
#             time.sleep(5)
#         else:
#             counter_oxy_left = 0
#             counter_oxy_right = 0
#             time_start = time.time()
#
#             while(time.time() - time_start < 3):
#                 query_rule_oxy_left = f"""
#                     SELECT 
#                         tbr.f_value,
#                         f_bracket_close
#                     FROM
#                         tb_combustion_rules_dtl tcrd
#                     JOIN tb_bat_raw tbr ON
#                         tbr.f_address_no = tcrd.f_tag_sensor
#                     WHERE
#                         tcrd.f_rule_hdr_id = 20
#                         AND tcrd.f_tag_sensor IN {tuple(tags)}
#                     ORDER BY
#                         tcrd.f_tag_sensor ASC"""
#
#                 df = pd.read_sql_query(query_rule_oxy_left, engine)
#                 df_dict = df.astype(str).to_dict('records')
#                 oxy_rule_left = df_dict[0]['f_value'] + df_dict[0]['f_bracket_close']
#                 oxy_rule_left = oxy_rule_left.lower().replace(')','').replace('and','')
#                 eval_oxy_left = eval(oxy_rule_left)
#
#                 oxy_rule_right = df_dict[1]['f_value'] + df_dict[1]['f_bracket_close']
#                 oxy_rule_right = oxy_rule_right.lower().replace(')','').replace('and','')
#                 eval_oxy_right = eval(oxy_rule_right)
#
#                 if eval_oxy_left:
#                     total_count_oxy_left = 0
#                     counter_oxy_left = 0
#                     write_to_opc(sg_oxy_l, False)
#                     g_x2_o2_l = False
#                 else:
#                     counter_oxy_left += 1
#
#                 if eval_oxy_right:
#                     total_count_oxy_right = 0
#                     counter_oxy_right = 0
#                     write_to_opc(sg_oxy_r, False)
#                     g_x2_o2_r = False
#                 else:
#                     counter_oxy_right += 1
#                 time.sleep(0.5)
#
#             if counter_oxy_left > 0:
#                 total_count_oxy_left += 1
#
#             if counter_oxy_right > 0:
#                 total_count_oxy_right += 1
#
#             if total_count_oxy_left >= 3:
#                 write_to_opc(sg_oxy_l, True)
#                 g_x2_o2_l = True
#
#             if total_count_oxy_right >= 3:
#                 write_to_opc(sg_oxy_r, True)
#                 g_x2_o2_r = True
#
# def bg_maintain_x2_o2l_true():
#     while True:
#         param_skip_dcs_sg_indicator = get_copt_parameter(db_config.PARAM_SKIP_DCS_SG_INDICATOR)
#         sg_oxy_l = get_constrain(db_config.SG_OXY_L)
#
#         if param_skip_dcs_sg_indicator == 1:
#             time.sleep(5)
#         else:
#             if g_x2_o2_l == True:
#                 time.sleep(0.5)
#                 x2_o2_l = get_tag_value(sg_oxy_l)
#
#                 if x2_o2_l == False:
#                     write_to_opc(sg_oxy_l, True)
#
# def bg_maintain_x2_o2r_true():
#     while True:
#         param_skip_dcs_sg_indicator = get_copt_parameter(db_config.PARAM_SKIP_DCS_SG_INDICATOR)
#         sg_oxy_r = get_constrain(db_config.SG_OXY_R)
#
#         if param_skip_dcs_sg_indicator == 1:
#             time.sleep(5)
#         else:
#             if g_x2_o2_r == True:
#                 time.sleep(0.5)
#                 x2_o2_r = get_tag_value(sg_oxy_r)
#
#                 if x2_o2_r == False:
#                     write_to_opc(sg_oxy_r, True)
                    
            
        
    


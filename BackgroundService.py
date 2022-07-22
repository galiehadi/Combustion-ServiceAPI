from operator import index
import pandas as pd
import numpy as np
import time, sqlalchemy, requests, config, re, traceback
from urllib.parse import quote_plus as urlparse
from pprint import pprint
from regional_regressor import RegionalLinearReg

_UNIT_CODE_ = config._UNIT_CODE_
_UNIT_NAME_ = config._UNIT_NAME_
_USER_ = config._USER_
_PASS_ = urlparse(config._PASS_)
_IP_ = config._IP_
_DB_NAME_ = config._DB_NAME_
_LOCAL_IP_ = config._LOCAL_IP_
_LOCAL_MODE_ = False

# Default values
DEBUG_MODE = True
dcs_x = config.DCS_X
dcs_y = config.DCS_Y
DCS_O2 = RegionalLinearReg(dcs_x, dcs_y)

con = f"mysql+mysqlconnector://{_USER_}:{_PASS_}@{_IP_}/{_DB_NAME_}"
engine = sqlalchemy.create_engine(con)
conn = engine.connect()

def logging(text):
    t = time.strftime('%Y-%m-%d %X (%z)')
    print(f"[{t}] - {text}")

def bg_update_notification():
    # Get status enable now
    q = f"""SELECT raw.f_address_no, raw.f_value, '' AS f_message, raw.f_updated_at FROM tb_tags_read_conf conf 
        LEFT JOIN tb_bat_raw raw
        ON conf.f_tag_name = raw.f_address_no 
        WHERE f_description = "{config.DESC_ENABLE_COPT}" """
    df_status_now = pd.read_sql(q, engine)
    tag_name, status_now, message, timestamp_now = df_status_now.iloc[0].values
    timestamp_now = pd.to_datetime(timestamp_now).strftime('%Y-%m-%d %X')

    # Get latest update on message
    q = f"""SELECT notif.f_value from tb_tags_read_conf conf
        LEFT JOIN tb_bat_notif notif 
        ON conf.f_tag_name = notif.f_address_no 
        WHERE conf.f_description = "COMBUSTION ENABLE"
        ORDER BY notif.f_updated_at DESC LIMIT 1"""
    df_status_last = pd.read_sql(q, engine)
    status_last = df_status_last['f_value'].values[0]

    if status_last != status_now:
        if int(status_now):
            message = f'COPT Enabled on {timestamp_now}'
        else:
            # Alarm messages
            q = f"""SELECT f_timestamp, f_desc, f_set_value, f_actual_value FROM tb_combustion_alarm_history
                WHERE f_timestamp > NOW() - INTERVAL 2 MINUTE
                ORDER BY f_timestamp DESC """
            df_alarm = pd.read_sql(q, engine)
            df_alarm = df_alarm.drop_duplicates(subset=['f_desc','f_set_value'], keep='first')

            alarm_message = ' \nSafeguard Violated:'
            for i in df_alarm.index:
                alarm_timestamp, _, alarm_set_val, alarm_act_val = df_alarm.loc[i]
                alarm_timestamp = pd.to_datetime(alarm_timestamp).strftime('%Y-%m-%d %X')
                alarm = f" \n[{alarm_timestamp}] - {alarm_set_val} ({alarm_act_val})"
                alarm_message += alarm

            message = f'COPT Disabled on {timestamp_now}.'
            if len(alarm_message) > 25:
                message += alarm_message
        q = f"""INSERT INTO tb_bat_notif (f_address_no,f_value,f_message,f_updated_at)
                VALUES ("{tag_name}", {status_now}, "{message}", "{timestamp_now}") """
        print(message)
        try:
            with engine.connect() as conn:
                conn.execute(q)
        except: pass

    return

def bg_combustion_safeguard_check():
    t0 = time.time()
    q = f"""SELECT
                NOW() AS timestamp,
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
                hdr.f_rule_descr = "SAFEGUARD" AND
                rule.f_is_active = 1
            ORDER BY
                rule.f_sequence"""
    sg = pd.read_sql(q, engine)
    ts = sg['timestamp'].max()

    Safeguard_status = True
    Safeguard_text = ''
    Alarms = []
    for i in sg.index:
        _, tagname, description, bracketOpen, value, bracketClose = sg.iloc[i]
        bracketClose = bracketClose.replace('==','=').replace("=","==")
        Safeguard_text += f"{bracketOpen}{value}{bracketClose} "

        bracketClose_ = bracketClose.replace('AND','').replace('OR','')
        individualRule = f"{bracketOpen}{value}{bracketClose_} ".lower()
        individualAlarm = {
            'f_timestamp': ts,
            'f_desc': 'Safeguard',
            'f_set_value': f"{bracketOpen}{description}{bracketClose_}",
            'f_actual_value':str(value),
            'f_rule_header': 20
        }
        try: 
            if not eval(individualRule): Alarms.append(individualAlarm)
        except:
            Alarms.append(individualAlarm)

    Safeguard_text = Safeguard_text.lower()
    Safeguard_status = eval(Safeguard_text)

    ret = {
        'Safeguard Status': Safeguard_status,
        'Execution time': str(round(time.time() - t0,3)) + ' sec',
        'Individual Alarm': Alarms
    }
    return ret

def bg_sootblow_safeguard_check():
    t0 = time.time()
    q = f"""SELECT
                NOW() AS timestamp,
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
                hdr.f_rule_descr = "SAFEGUARD" AND
                rule.f_is_active = 1
            ORDER BY
                rule.f_sequence"""
    sg = pd.read_sql(q, engine)
    ts = sg['timestamp'].max()

    Safeguard_status = True
    Safeguard_text = ''
    Alarms = []
    for i in sg.index:
        _, tagname, description, bracketOpen, value, bracketClose = sg.iloc[i]
        bracketClose = bracketClose.replace('==','=').replace("=","==")
        Safeguard_text += f"{bracketOpen}{value}{bracketClose} "

    Safeguard_text = Safeguard_text.lower()
    Safeguard_status = eval(Safeguard_text)

    ret = {
        'Safeguard Status': Safeguard_status,
        'Execution time': str(round(time.time() - t0,3)) + ' sec',
        'Individual Alarm': Alarms
    }
    return ret

# TODO: Reconstruct safeguard updates
def bg_safeguard_update():
    S_COPT = bg_combustion_safeguard_check()
    S_SOPT = bg_sootblow_safeguard_check()
    copt_safeguard_status = S_COPT['Safeguard Status']
    sopt_safeguard_status = S_SOPT['Safeguard Status']

    O2_tag, GrossMW_tag, COPTenable_name = ['excess_o2', 'generator_gross_load', config.DESC_ENABLE_COPT]

    # Always update COPT safeguard status
    q = f"""UPDATE {_DB_NAME_}.tb_bat_raw SET f_date_rec=NOW(), f_value={1 if copt_safeguard_status else 0}, f_updated_at=NOW()
            WHERE f_address_no = "{config.SAFEGUARD_TAG}" """
    with engine.connect() as conn:
        res = conn.execute(q)
        
    # Always update SOPT safeguard status
    q = f"""UPDATE {_DB_NAME_}.tb_bat_raw SET f_date_rec=NOW(), f_value={1 if sopt_safeguard_status else 0}, f_updated_at=NOW()
            WHERE f_address_no = "{config.SAFEGUARD_SOPT_TAG}" """
    with engine.connect() as conn:
        res = conn.execute(q)

    # Get current condition
    q = f"""SELECT NOW() AS f_date_rec, f_description as name, raw.f_value FROM {_DB_NAME_}.tb_tags_read_conf conf
            LEFT JOIN {_DB_NAME_}.tb_bat_raw raw
            ON conf.f_tag_name = raw.f_address_no 
            WHERE conf.f_description = "{config.DESC_ENABLE_COPT}" 
            UNION 
            SELECT NOW() AS f_date_rec, f_address_no AS name, f_value FROM {_DB_NAME_}.tb_bat_raw raw
            WHERE f_address_no = "{config.SAFEGUARD_TAG}"
            UNION
            SELECT NOW() AS f_date_rec, disp.f_desc AS name, raw.f_value FROM {_DB_NAME_}.cb_display disp
            LEFT JOIN {_DB_NAME_}.tb_bat_raw raw
            ON disp.f_tags = raw.f_address_no 
            WHERE disp.f_desc IN ("{O2_tag}", "{GrossMW_tag}") """
    df = pd.read_sql(q, engine)
    ts = df['f_date_rec'].max()
    df = df.set_index('name')['f_value']
    safeguard_current = bool(df[config.SAFEGUARD_TAG])
    combustion_enable = bool(df[config.DESC_ENABLE_COPT])
    mw_current = df[GrossMW_tag]
    o2_current = df[O2_tag]

    # If combustion is enabled and safeguard is down, disable the recommendations, revert back to its original condition
    # and append alarm history
    if combustion_enable and not copt_safeguard_status:
        # Send alarm to OPC
        q = f"""SELECT f_tag_name FROM {_DB_NAME_}.tb_tags_read_conf conf
                WHERE f_description = "Excess O2" """
        o2_recom_tag = pd.read_sql(q, engine).values[0][0]

        logging('Some of safeguards are violated. Turning off COPT ...')
        
        # Revert all changes
        copt_enable = df[COPTenable_name].max()
        o2_bias = o2_current - DCS_O2.predict(mw_current)

        opc_write = [[o2_recom_tag, ts, o2_bias]]
        opc_write = pd.DataFrame(opc_write, columns=['tag_name','ts','value'])
        
        opc_write.to_sql('tb_opc_write_copt', con, if_exists='append', index=False)
        opc_write.to_sql('tb_opc_write_history_copt', engine, if_exists='append', index=False)

        # Append alarm history
        Alarms = S_COPT['Individual Alarm']
        AlarmDF = pd.DataFrame(Alarms)
        AlarmDF.to_sql('tb_combustion_alarm_history', engine, if_exists='append', index=False)

        # Write alarm 102 to DCS 
        for table in ['tb_opc_write_copt','tb_opc_write_history_copt']:
            q = f"""INSERT IGNORE INTO {_DB_NAME_}.{table}
                    SELECT f_tag_name AS tag_name, NOW() AS ts, 102 AS value FROM {_DB_NAME_}.tb_tags_write_conf ttwc 
                    WHERE f_description = "{config.DESC_ALARM}" """
            with engine.connect() as conn:
                res = conn.execute(q)
    
    if copt_safeguard_status:
        # Checking last alarm
        q = f"""SELECT value FROM {_DB_NAME_}.tb_opc_write_history_copt
                WHERE tag_name = (SELECT conf.f_tag_name FROM {_DB_NAME_}.tb_tags_write_conf conf
                                WHERE f_description = "{config.DESC_ALARM}")
                ORDER BY ts DESC
                LIMIT 1"""
        alarm_current_status = pd.read_sql(q, engine)
        if len(alarm_current_status) > 0: alarm_current_status = int(alarm_current_status.values)
        else: alarm_current_status = 102
        if alarm_current_status != 100:
            # Write back alarm 100 to DCS 
            for table in ['tb_opc_write_copt','tb_opc_write_history_copt']:
                q = f"""INSERT IGNORE INTO {_DB_NAME_}.{table}
                        SELECT f_tag_name AS tag_name, NOW() AS ts, 100 AS value FROM {_DB_NAME_}.tb_tags_write_conf ttwc 
                        WHERE f_description = "{config.DESC_ALARM}" """
                with engine.connect() as conn:
                    res = conn.execute(q)
            logging(f'Write to OPC: {config.DESC_ALARM}: 102 changed to 100')
    return S_COPT


def bg_get_recom_exec_interval():
    q = f"""SELECT f_default_value FROM {_DB_NAME_}.tb_combustion_parameters tcp 
            WHERE f_label = 'RECOM_EXEC_INTERVAL' """
    df = pd.read_sql(q, engine)
    recom_exec_interval = float(df.values)
    return recom_exec_interval

# Write recommendation periodical from operator parameters
def bg_write_recommendation_to_opc(MAX_BIAS_PERCENTAGE):
    q = f"""SELECT conf.f_description, raw.f_value FROM {_DB_NAME_}.tb_bat_raw raw
            LEFT JOIN {_DB_NAME_}.tb_tags_read_conf conf
            ON raw.f_address_no = conf.f_tag_name
            WHERE conf.f_category LIKE "%ENABLE%" 
            AND conf.f_is_active = 1
            """
    Enable_status_df = pd.read_sql(q, engine).set_index('f_description')['f_value']
    Enable_status_df = Enable_status_df.replace(np.nan, 0)

    # Enable tags
    q = f"""SELECT f_category, f_description, f_tag_name FROM {_DB_NAME_}.tb_tags_write_conf
            WHERE f_tag_use = "COPT"
            AND f_is_active = 1 """
    Write_tags = pd.read_sql(q, engine)

    Enable_status = {}
    for c in [config.DESC_ENABLE_COPT_BT, config.DESC_ENABLE_COPT_SEC, config.DESC_ENABLE_COPT_MOT]:
        status = int(Enable_status_df[c]) if c in Enable_status_df.index else 0
        tags = Write_tags[Write_tags['f_category'] == c]['f_tag_name'].values.tolist() if c in np.unique(Write_tags['f_category']) else []
        Enable_status[c] = {
            'status': status,
            'tag_lists': tags
        }

    # Reading latest recommendation
    q = f"""SELECT gen.model_id, gen.ts, conf.f_tag_name, conf.f_description, gen.value, gen.bias_value, gen.enable_status, 
            gen.value - gen.bias_value AS current_value
            FROM tb_combustion_model_generation gen
            LEFT JOIN tb_tags_write_conf conf 
            ON gen.tag_name = conf.f_description 
            WHERE gen.ts = (SELECT MAX(ts) FROM tb_combustion_model_generation gen)
            GROUP BY conf.f_description """
    Recom = pd.read_sql(q, engine)
    Recom['bias_value'] = Recom['value'] - Recom['current_value']

    # Periodical commands
    q = f"SELECT f_default_value FROM tb_combustion_parameters WHERE f_label = 'COMMAND_PERIOD'"
    COMMAND_PERIOD = int(pd.read_sql(q, engine).values[0][0])

    ts = Recom['ts'].max()
    ct = pd.to_datetime(time.ctime())
    recom_ke = (ct - ts).components.minutes + 1
    if recom_ke > COMMAND_PERIOD: return 'Waiting'
    Recom['bias_value'] = Recom['bias_value'] * recom_ke / COMMAND_PERIOD
    
    o2_idx = None
    # Limit recommendation to MAX_BIAS_PERCENTAGE %
    for i in Recom.index:
        mxv = MAX_BIAS_PERCENTAGE * abs(Recom.loc[i, 'current_value']) / 100
        Recom.loc[i, 'bias_value'] = max(-mxv, Recom.loc[i, 'bias_value'])
        Recom.loc[i, 'bias_value'] = min(mxv, Recom.loc[i, 'bias_value'])
        if 'Oxygen' in Recom.loc[i, 'f_description']: o2_idx = i
        elif 'O2' in Recom.loc[i, 'f_description']: o2_idx = i
    Recom['value'] = Recom['current_value'] + Recom['bias_value']

    # Calculate O2 Set Point based on GrossMW from DCS
    q = f"""SELECT f_value FROM {_DB_NAME_}.cb_display disp
            LEFT JOIN {_DB_NAME_}.tb_bat_raw raw
            on disp.f_tags = raw.f_address_no 
            WHERE f_desc = "generator_gross_load" """
    dcs_mw = pd.read_sql(q, engine).values[0][0]
    dcs_o2 = DCS_O2.predict(dcs_mw)

    opc_write = Recom.merge(Write_tags, how='left', left_on='f_description', right_on='f_description')
    opc_write = opc_write[['f_tag_name_y', 'f_description','ts',config.PARAMETER_BIAS, config.PARAMETER_SET_POINT]].dropna()
    opc_write['value_to_send'] = 0
    for i in opc_write.index:
        desc = opc_write.loc[i, 'f_description']
        if desc in config.PARAMETER_WRITE.keys():
            opc_write.loc[i, 'value_to_send'] = opc_write.loc[i, config.PARAMETER_SET_POINT] if config.PARAMETER_WRITE[desc] == config.PARAMETER_SET_POINT else opc_write.loc[i, config.PARAMETER_BIAS]
    opc_write = opc_write[['f_tag_name_y', 'ts', 'value_to_send']]

    opc_write.columns = ['tag_name','ts','value']
    opc_write['ts'] = pd.to_datetime(time.ctime())

    if o2_idx is not None:
        opc_write.loc[o2_idx, 'value'] = opc_write.loc[o2_idx, 'value'] - dcs_o2

    # # Remove tags that disabled partially
    # for C in Enable_status.keys():
    #     if not bool(Enable_status[C]['status']):
    #         tags = Enable_status[C]['tag_lists']
    #         opc_write = opc_write.drop(index = opc_write[opc_write['tag_name'].isin(tags)].index)
    
    opc_write.to_sql('tb_opc_write_copt', engine, if_exists='append', index=False)
    opc_write.to_sql('tb_opc_write_history_copt', engine, if_exists='append', index=False)
    logging(f'Write to OPC: \n{opc_write}\n')
    return 'Done!'
    

# Write recommendation slowly with reading realtime data
def bg_write_recommendation_to_opc1(MAX_BIAS_PERCENTAGE):
    # Enable Status
    q = f"""SELECT conf.f_description, raw.f_value FROM {_DB_NAME_}.tb_bat_raw raw
            LEFT JOIN {_DB_NAME_}.tb_tags_read_conf conf
            ON raw.f_address_no = conf.f_tag_name
            WHERE conf.f_category LIKE "%ENABLE%" 
            AND conf.f_is_active = 1
            """
    Enable_status_df = pd.read_sql(q, engine).set_index('f_description')['f_value']
    Enable_status_df = Enable_status_df.replace(np.nan, 0)

    # Enable tags
    q = f"""SELECT f_category, f_description, f_tag_name FROM {_DB_NAME_}.tb_tags_write_conf
            WHERE f_tag_use = "COPT" """
    Write_tags = pd.read_sql(q, engine)
    Write_tags

    Enable_status = {}
    for c in [config.DESC_ENABLE_COPT_BT, config.DESC_ENABLE_COPT_SEC, config.DESC_ENABLE_COPT_MOT]:
        status = int(Enable_status_df[c]) if c in Enable_status_df.index else 0
        tags = Write_tags[Write_tags['f_category'] == c]['f_tag_name'].values.tolist() if c in np.unique(Write_tags['f_category']) else []
        Enable_status[c] = {
            'status': status,
            'tag_lists': tags
        }

    # Limit recommendations to +- MAX_BIAS_PERCENTAGE %
    q = f"""SELECT gen.model_id, gen.ts, conf.f_tag_name, conf.f_description, gen.value, gen.bias_value, gen.enable_status, 
            (CASE WHEN gen.tag_name = "Total Secondary Air Flow" THEN AVG(raw.f_value*2) ELSE raw.f_value END) AS current_value
            FROM tb_combustion_model_generation gen
            LEFT JOIN tb_tags_read_conf conf
            ON gen.tag_name = conf.f_description 
            LEFT JOIN tb_bat_raw raw 
            ON conf.f_tag_name = raw.f_address_no 
            WHERE gen.ts = (SELECT MAX(ts) FROM tb_combustion_model_generation gen)
            AND conf.f_category != "Recommendation"
            AND conf.f_is_active = 1
            GROUP BY gen.tag_name"""
    Recom = pd.read_sql(q, engine)
    Recom['bias_value'] = Recom['value'] - Recom['current_value']

    o2_idx = None
    # Limit recommendation to MAX_BIAS_PERCENTAGE %
    for i in Recom.index:
        mxv = MAX_BIAS_PERCENTAGE * abs(Recom.loc[i, 'current_value']) / 100
        Recom.loc[i, 'bias_value'] = max(-mxv, Recom.loc[i, 'bias_value'])
        Recom.loc[i, 'bias_value'] = min(mxv, Recom.loc[i, 'bias_value'])
        if 'Oxygen' in Recom.loc[i, 'f_description']: o2_idx = i
        elif 'O2' in Recom.loc[i, 'f_description']: o2_idx = i
    Recom['value'] = Recom['current_value'] + Recom['bias_value']

    # Calculate O2 Set Point based on GrossMW from DCS
    q = f"""SELECT f_value FROM {_DB_NAME_}.cb_display disp
            LEFT JOIN {_DB_NAME_}.tb_bat_raw raw
            on disp.f_tags = raw.f_address_no 
            WHERE f_desc = "generator_gross_load" """
    dcs_mw = pd.read_sql(q, engine).values[0][0]
    dcs_o2 = DCS_O2.predict(dcs_mw)

    opc_write = Recom.merge(Write_tags, how='left', left_on='f_description', right_on='f_description')[['f_tag_name_y','ts','value']].dropna()
    opc_write.columns = ['tag_name','ts','value']
    opc_write['ts'] = pd.to_datetime(time.ctime())

    if o2_idx is not None:
        opc_write.loc[o2_idx, 'value'] = opc_write.loc[o2_idx, 'value'] - dcs_o2

    # Remove tags that disabled partially
    for C in Enable_status.keys():
        if not bool(Enable_status[C]['status']):
            tags = Enable_status[C]['tag_lists']
            opc_write = opc_write.drop(index = opc_write[opc_write['tag_name'].isin(tags)].index)
    
    opc_write.to_sql('tb_opc_write_copt', engine, if_exists='append', index=False)
    opc_write.to_sql('tb_opc_write_history_copt', engine, if_exists='append', index=False)
    logging(f'Write to OPC: {opc_write}')
    return 'Done!'

def bg_get_ml_model_status():
    q = f"""SELECT message FROM {_DB_NAME_}.tb_combustion_model_message 
        WHERE ts = (SELECT MAX(ts) FROM {_DB_NAME_}.tb_combustion_model_message)
        AND ts > (NOW() - INTERVAL 1 HOUR) """
    message = pd.read_sql(q, engine)
    if len(message) > 0:
        message = message.values[0][0]
        code = message.split(':')[0]
        code = int(re.findall('[0-9]+', code)[0])
        return code
    return 'Failed'

def bg_get_ml_recommendation():
    try:
        now = pd.to_datetime(time.ctime())
        logging('Running bg_get_ml_recommendation')

        # Calling ML Recommendations to the latest recommendation
        # TODO: Set latest COPT call based on timestamp
        q = f"""SELECT f_date_rec, f_value FROM {_DB_NAME_}.tb_bat_raw
                WHERE f_address_no = "{config.TAG_COPT_ISCALLING}" """
        copt_is_calling_timestamp, copt_is_calling = pd.read_sql(q, engine).values[0]
        if not copt_is_calling:
            logging('Calling COPT ...')
            q = f"""UPDATE {_DB_NAME_}.tb_bat_raw
                    SET f_value=1,f_date_rec=NOW(),f_updated_at=NOW()
                    WHERE f_address_no='{config.TAG_COPT_ISCALLING}' """
            # q = f"""REPLACE INTO {_DB_NAME_}.tb_bat_raw
            #         (f_value, f_date_rec, f_updated_at, f_address_no) VALUES
            #         (1, NOW(), NOW(), "{config.TAG_COPT_ISCALLING}") """
            with engine.connect() as conn:
                res = conn.execute(q)

            url = f'http://{_LOCAL_IP_}/bat_combustion/{_UNIT_CODE_}/realtime'
            logging(f"Calling COPT on URL: {url}")
            response = requests.get(url)

            q = f"""UPDATE {_DB_NAME_}.tb_bat_raw
                    SET f_value=0,f_date_rec=NOW(),f_updated_at=NOW()
                    WHERE f_address_no='{config.TAG_COPT_ISCALLING}' """
            # q = f"""REPLACE INTO {_DB_NAME_}.tb_bat_raw
            #         (f_value, f_date_rec, f_updated_at, f_address_no) VALUES
            #         (1, NOW(), NOW(), "{config.TAG_COPT_ISCALLING}") """
            with engine.connect() as conn:
                res = conn.execute(q)
            
            ret = response.json()
            return ret
        elif (now - copt_is_calling_timestamp) > pd.Timedelta('60sec'):
            # Set back COPT_is_calling to 0 if last update > 60 sec ago.
            logging("Set back COPT_is_calling to 0 cause a timeout.")
            q = f"""UPDATE {_DB_NAME_}.tb_bat_raw
                    SET f_value=0,f_date_rec=NOW(),f_updated_at=NOW()
                    WHERE f_address_no='{config.TAG_COPT_ISCALLING}' """
            with engine.connect() as conn:
                res = conn.execute(q)
    except Exception as e:
        logging(f'Machine learning prediction error: {traceback.format_exc()}')
        return str(e)

def bg_ml_runner():
    ENABLE_COPT = 0
    MAX_BIAS_PERCENTAGE = 5
    RECOM_EXEC_INTERVAL = 15
    LATEST_RECOMMENDATION_TIME = pd.to_datetime('2020-01-01 00:00')

    t0 = time.time()

    # Get Enable status
    q = f"""SELECT conf.f_description, raw.f_value FROM {_DB_NAME_}.tb_tags_read_conf conf
            LEFT JOIN {_DB_NAME_}.tb_bat_raw raw
            ON conf.f_tag_name = raw.f_address_no 
            WHERE conf.f_description IN ("{config.DESC_ENABLE_COPT}",
            "{config.DESC_ENABLE_COPT_BT}","{config.DESC_ENABLE_COPT_SEC}") """
    df = pd.read_sql(q, engine).set_index('f_description')['f_value']
    ENABLE_COPT = df[config.DESC_ENABLE_COPT]
    ENABLE_COPT_BT = df[config.DESC_ENABLE_COPT_BT]
    ENABLE_COPT_SEC = df[config.DESC_ENABLE_COPT_SEC]

    # Get parameters
    q = f"""SELECT f_label, f_default_value FROM {_DB_NAME_}.tb_combustion_parameters tcp 
            WHERE f_label IN ("MAX_BIAS_PERCENTAGE","RECOM_EXEC_INTERVAL","DEBUG_MODE") """
    parameters = pd.read_sql(q, engine).set_index('f_label')['f_default_value']

    if 'MAX_BIAS_PERCENTAGE' in parameters.index:
        MAX_BIAS_PERCENTAGE = float(parameters['MAX_BIAS_PERCENTAGE'])
    if 'RECOM_EXEC_INTERVAL' in parameters.index:
        RECOM_EXEC_INTERVAL = int(parameters['RECOM_EXEC_INTERVAL'])
    if 'DEBUG_MODE' in parameters.index:
        DEBUG_MODE = False if (str(int(parameters['DEBUG_MODE'])).lower() in ['0','false',0]) else True
    
    logging(f'DEBUG_MODE : {DEBUG_MODE}')
    logging(f'COPT ENABLE: {bool(ENABLE_COPT)}')
    
    if DEBUG_MODE:
        if ENABLE_COPT:
            # Change DEBUG_MODE to False
            q = f"""UPDATE {_DB_NAME_}.tb_combustion_parameters
                    SET f_default_value=0
                    WHERE f_label="DEBUG_MODE";"""
            with engine.connect() as conn:
                conn.execute(q)
        else:
            # Get latest recommendations time
            q = f"""SELECT MAX(ts) FROM {_DB_NAME_}.tb_combustion_model_generation"""
            df = pd.read_sql(q, engine)
            try: LATEST_RECOMMENDATION_TIME = pd.to_datetime(df.values[0][0])
            except Exception as e: logging(f"Error getting latest recommendation:", str(e)) 

            # Return if latest recommendation is under RECOM_EXEC_INTERVAL minute
            now = pd.to_datetime(time.ctime())
            if (now - LATEST_RECOMMENDATION_TIME) < pd.Timedelta(f'{RECOM_EXEC_INTERVAL}min'):
                return {'message':f"Waiting to next {LATEST_RECOMMENDATION_TIME + pd.Timedelta(f'{RECOM_EXEC_INTERVAL}min') - now} min"}
            
            # Calling ML Recommendations to the latest recommendation
            val = bg_get_ml_recommendation()
            return {'message': f"Value: {val}"}

    elif ENABLE_COPT:
        # Get latest recommendations time
        q = f"""SELECT MAX(ts) FROM {_DB_NAME_}.tb_combustion_model_generation"""
        df = pd.read_sql(q, engine)
        try: LATEST_RECOMMENDATION_TIME = pd.to_datetime(df.values[0][0])
        except Exception as e: logging(f"Error getting latest recommendation: {e}") 

        now = pd.to_datetime(time.ctime())
        # TEMPORARY! 
        if (now - LATEST_RECOMMENDATION_TIME) < pd.Timedelta(f'{RECOM_EXEC_INTERVAL}min'):
            # TODO: make a smooth transition recommendation
            logging(f"Last recommendation was {(now - LATEST_RECOMMENDATION_TIME)} ago. Sending recommendation values to OPC smoothly.")
            # Checking current O2 level
            q = f"""SELECT raw.f_value FROM {_DB_NAME_}.cb_display disp
                    LEFT JOIN {_DB_NAME_}.tb_bat_raw raw
                    ON disp.f_tags = raw.f_address_no 
                    WHERE disp.f_desc = "excess_o2" """
            current_oxygen = pd.read_sql(q, engine).values[0][0]
            # Latest recommendation
            q = f"""SELECT gen.model_id, gen.ts, conf.f_tag_name, conf.f_description, 
                    gen.value, gen.bias_value, gen.enable_status, gen.value - gen.bias_value AS 'current_value' 
                    FROM {_DB_NAME_}.tb_tags_read_conf conf
                    LEFT JOIN {_DB_NAME_}.tb_combustion_model_generation gen
                    ON conf.f_description = gen.tag_name 
                    WHERE f_category = "Recommendation"
                    AND gen.ts = (SELECT MAX(ts) FROM {_DB_NAME_}.tb_combustion_model_generation tcmg)"""
            Recom = pd.read_sql(q, engine)
            set_point_oxygen = float(Recom[Recom['f_description'] == 'Excess Oxygen Sensor']['value'])
            if (abs(set_point_oxygen - current_oxygen) < config.OXYGEN_STEADY_STATE_LEVEL): 
                logging(f'Oxygen is in steady state level ({current_oxygen}).')
                return {'message': 'Oxygen is in steady state level.'}
            
            # Write recommendation to OPC
            bg_write_recommendation_to_opc(MAX_BIAS_PERCENTAGE)
            return {'message':f"Waiting to next {LATEST_RECOMMENDATION_TIME + pd.Timedelta(f'{RECOM_EXEC_INTERVAL}min') - now} min"}
        
        else:
            # Calling ML Recommendations to the latest recommendation
            logging(f"Last recommendation was {(now - LATEST_RECOMMENDATION_TIME)} ago. Generating new ")
            ML = bg_get_ml_recommendation()

            if type(ML) is dict: 
                if 'model_status' not in ML.keys():
                    try:
                        ML['model_status'] = int(bg_get_ml_model_status())
                    except:
                        return {'message':'Error on ML response. Columns "model_status" not found.'}

                elif ML['model_status'] == 1:
                    try:
                        bg_write_recommendation_to_opc(MAX_BIAS_PERCENTAGE)
                    except Exception as e:
                        return {'message': str(e)}
                    return {'message':'Done!'}
            else:
                return {'message': f"Error! Message: {ML}"}

if _LOCAL_MODE_:
    k = bg_ml_runner()
    print(time.strftime('%X\t'), k)

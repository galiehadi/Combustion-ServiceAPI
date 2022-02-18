from operator import index
import pandas as pd
import numpy as np
import time, sqlalchemy, requests, config
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

# _LOCAL_MODE_ = True
# if _LOCAL_MODE_:
#     _IP_ = 'localhost:3308'
#     _LOCAL_IP_ = 'localhost'

# Default values
DEBUG_MODE = True
dcs_x = [0, 150, 255, 300, 330]
dcs_y = [8, 6.0, 4.5, 4.0, 4.0]
DCS_O2 = RegionalLinearReg(dcs_x, dcs_y)

con = f"mysql+mysqlconnector://{_USER_}:{_PASS_}@{_IP_}/{_DB_NAME_}"
engine = sqlalchemy.create_engine(con)

def logging(text):
    t = time.strftime('%Y-%m-%d %X')
    print(f"[{t}] - {text}")

def bg_safeguard_check():
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
                hdr.f_rule_descr = "SAFEGUARD"
            ORDER BY
                rule.f_sequence"""
    sg = pd.read_sql(q, con)
    ts = sg['timestamp'].max()

    Safeguard_status = True
    Safeguard_text = ''
    Alarms = []
    for i in sg.index:
        _, tagname, description, bracketOpen, value, bracketClose = sg.iloc[i]
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

# TODO: Reconstruct safeguard updates
def bg_safeguard_update():
    S = bg_safeguard_check()
    safeguard_status = S['Safeguard Status']

    O2_tag, GrossMW_tag, COPTenable_name = ['excess_o2', 'generator_gross_load', config.DESC_ENABLE_COPT]

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
    df = pd.read_sql(q, con)
    ts = df['f_date_rec'].max()
    df = df.set_index('name')['f_value']
    safeguard_current = bool(df[config.SAFEGUARD_TAG])
    combustion_enable = bool(df[config.DESC_ENABLE_COPT])
    mw_current = df[GrossMW_tag]
    o2_current = df[O2_tag]

    # Always update safeguard status
    q = f"""UPDATE {_DB_NAME_}.tb_bat_raw SET f_date_rec=NOW(), f_value={1 if safeguard_status else 0}, f_updated_at=NOW()
            WHERE f_address_no = "{config.SAFEGUARD_TAG}" """
    with engine.connect() as conn:
        res = conn.execute(q)

    # If combustion is enabled and safeguard is down, disable the recommendations, revert back to its original condition
    # and append alarm history
    if combustion_enable and not safeguard_status:
        # Disable COPT
        q = f"""UPDATE {_DB_NAME_}.tb_bat_raw SET f_date_rec = NOW(), f_value = 0, f_updated_at = NOW()
                WHERE f_address_no = (SELECT conf.f_tag_name FROM {_DB_NAME_}.tb_tags_read_conf conf
                                    WHERE f_description = "{config.DESC_ENABLE_COPT}")"""
        with engine.connect() as conn:
            res = conn.execute(q)

        copt_enable = df[COPTenable_name].max()
        o2_bias = o2_current - DCS_O2.predict(mw_current)

        q = f"""SELECT f_tag_name FROM {_DB_NAME_}.tb_tags_read_conf conf
                WHERE f_description = "Excess Oxygen Sensor" """
        o2_recom_tag = pd.read_sql(q, con).values[0][0]

        logging('Some of safeguards are violated. Turning off COPT ...')
        
        opc_write = [[o2_recom_tag, ts, o2_bias]]
        opc_write = pd.DataFrame(opc_write, columns=['tag_name','ts','value'])
        
        opc_write.to_sql('tb_opc_write', con, if_exists='append', index=False)
        opc_write.to_sql('tb_opc_write_history', con, if_exists='append', index=False)

        # Append alarm history
        Alarms = S['Individual Alarm']
        AlarmDF = pd.DataFrame(Alarms)
        AlarmDF.to_sql('tb_combustion_alarm_history', con, if_exists='append', index=False)


def bg_get_recom_exec_interval():
    q = f"""SELECT f_default_value FROM {_DB_NAME_}.tb_combustion_parameters tcp 
            WHERE f_label = 'RECOM_EXEC_INTERVAL' """
    df = pd.read_sql(q, con)
    recom_exec_interval = float(df.values)
    return recom_exec_interval

def bg_write_recommendation_to_opc(MAX_BIAS_PERCENTAGE):
    # Limit recommendations to +- MAX_BIAS_PERCENTAGE %
    q = f"""SELECT gen.model_id, gen.ts, conf.f_tag_name, conf.f_description, 
            gen.value, gen.bias_value, gen.enable_status, gen.value - gen.bias_value AS 'current_value' 
            FROM {_DB_NAME_}.tb_tags_read_conf conf
            LEFT JOIN {_DB_NAME_}.tb_combustion_model_generation gen
            ON conf.f_description = gen.tag_name 
            WHERE f_category = "Recommendation"
            AND gen.ts = (SELECT MAX(ts) FROM {_DB_NAME_}.tb_combustion_model_generation tcmg)"""
    Recom = pd.read_sql(q, con)
    
    o2_idx = None
    # Limit recommendation to MAX_BIAS_PERCENTAGE %
    for i in Recom.index:
        mxv = MAX_BIAS_PERCENTAGE * abs(Recom.loc[i, 'current_value']) / 100
        Recom.loc[i, 'bias_value'] = max(-mxv, Recom.loc[i, 'bias_value'])
        Recom.loc[i, 'bias_value'] = min(mxv, Recom.loc[i, 'bias_value'])
        if 'Oxygen' in Recom.loc[i, 'f_description']: o2_idx = i
    Recom['value'] = Recom['current_value'] + Recom['bias_value']
    
    # Calculate O2 Set Point based on GrossMW from DCS
    q = f"""SELECT f_value FROM {_DB_NAME_}.cb_display disp
            LEFT JOIN {_DB_NAME_}.tb_bat_raw raw
            on disp.f_tags = raw.f_address_no 
            WHERE f_desc = "generator_gross_load" """
    dcs_mw = pd.read_sql(q, con).values[0][0]
    dcs_o2 = DCS_O2.predict(dcs_mw)

    opc_write = Recom[['f_tag_name','ts','value']]
    opc_write.columns = ['tag_name','ts','value']
    
    if o2_idx is not None:
        opc_write.loc[o2_idx, 'value'] = opc_write.loc[o2_idx, 'value'] - dcs_o2
    
    opc_write.to_sql('tb_opc_write', con, if_exists='append', index=False)
    opc_write.to_sql('tb_opc_write_history', con, if_exists='append', index=False)
    logging(f'Write to OPC: {opc_write}')
    return 'Done!'

def bg_get_ml_recommendation():
    try:
        now = pd.to_datetime(time.ctime())

        # Calling ML Recommendations to the latest recommendation
        # TODO: Set latest COPT call based on timestamp
        q = f"""SELECT f_date_rec, f_value FROM {_DB_NAME_}.tb_bat_raw
                WHERE f_address_no = "{config.TAG_COPT_ISCALLING}" """
        copt_is_calling_timestamp, copt_is_calling = pd.read_sql(q, con).values[0]
        if not copt_is_calling:
            logging('Calling COPT ...')
            q = f"""UPDATE {_DB_NAME_}.tb_bat_raw
                    SET f_value=1,f_date_rec=NOW(),f_updated_at=NOW()
                    WHERE f_address_no='{config.TAG_COPT_ISCALLING}' """
            with engine.connect() as conn:
                res = conn.execute(q)

            response = requests.get(f'http://{_LOCAL_IP_}:5002/bat_combustion/{_UNIT_CODE_}/realtime')

            q = f"""UPDATE {_DB_NAME_}.tb_bat_raw
                    SET f_value=0,f_date_rec=NOW(),f_updated_at=NOW()
                    WHERE f_address_no='{config.TAG_COPT_ISCALLING}' """
            with engine.connect() as conn:
                res = conn.execute(q)
            
            ret = response.json()
            return ret
        elif (now - copt_is_calling_timestamp) > pd.Timedelta('60sec'):
            # Set back COPT_is_calling to 0 if last update > 60 sec ago.
            q = f"""UPDATE {_DB_NAME_}.tb_bat_raw
                    SET f_value=0,f_date_rec=NOW(),f_updated_at=NOW()
                    WHERE f_address_no='{config.TAG_COPT_ISCALLING}' """
            with engine.connect() as conn:
                res = conn.execute(q)
    except Exception as e:
        logging(time.ctime(),'- Machine learning prediction error:', e)
        return str(e)

def bg_ml_runner():
    ENABLE_COPT = 0
    MAX_BIAS_PERCENTAGE = 5
    RECOM_EXEC_INTERVAL = 15
    LATEST_RECOMMENDATION_TIME = pd.to_datetime('2020-01-01 00:00')

    t0 = time.time()

    # Get Enable status
    q = f"""SELECT raw.f_value FROM {_DB_NAME_}.tb_tags_read_conf conf
            LEFT JOIN {_DB_NAME_}.tb_bat_raw raw
            ON conf.f_tag_name = raw.f_address_no 
            WHERE conf.f_description = "{config.DESC_ENABLE_COPT}" """
    df = pd.read_sql(q, con)
    ENABLE_COPT = df.values[0][0]

    # Get parameters
    q = f"""SELECT f_label, f_default_value FROM {_DB_NAME_}.tb_combustion_parameters tcp 
            WHERE f_label IN ("MAX_BIAS_PERCENTAGE","RECOM_EXEC_INTERVAL","DEBUG_MODE") """
    parameters = pd.read_sql(q, con).set_index('f_label')['f_default_value']

    if 'MAX_BIAS_PERCENTAGE' in parameters.index:
        MAX_BIAS_PERCENTAGE = float(parameters['MAX_BIAS_PERCENTAGE'])
    if 'RECOM_EXEC_INTERVAL' in parameters.index:
        RECOM_EXEC_INTERVAL = int(parameters['RECOM_EXEC_INTERVAL'])
    if 'DEBUG_MODE' in parameters.index:
        DEBUG_MODE = False if (parameters['DEBUG_MODE'].lower() in ['0','false',0]) else True
    
    print(f'DEBUG_MODE: {DEBUG_MODE}')
    
    if DEBUG_MODE:
        # Get latest recommendations time
        q = f"""SELECT MAX(ts) FROM {_DB_NAME_}.tb_combustion_model_generation"""
        df = pd.read_sql(q, con)
        try: LATEST_RECOMMENDATION_TIME = pd.to_datetime(df.values[0][0])
        except Exception as e: logging(f"Error on line 241:", str(e)) 

        # Return if latest recommendation is under RECOM_EXEC_INTERVAL minute
        now = pd.to_datetime(time.ctime())
        if (now - LATEST_RECOMMENDATION_TIME) < pd.Timedelta(f'{RECOM_EXEC_INTERVAL}min'):
            return {'message':f"Waiting to next {LATEST_RECOMMENDATION_TIME + pd.Timedelta(f'{RECOM_EXEC_INTERVAL}min') - now} min"}
        
        # Calling ML Recommendations to the latest recommendation
        val = bg_get_ml_recommendation()

    elif ENABLE_COPT:
        # Get latest recommendations time
        q = f"""SELECT MAX(ts) FROM {_DB_NAME_}.tb_combustion_model_generation"""
        df = pd.read_sql(q, con)
        try: LATEST_RECOMMENDATION_TIME = pd.to_datetime(df.values[0][0])
        except Exception as e: logging(f"Error on line 256:", str(e)) 

        now = pd.to_datetime(time.ctime())
        # TEMPORARY! 
        if (now - LATEST_RECOMMENDATION_TIME) < pd.Timedelta(f'{RECOM_EXEC_INTERVAL}min'):
            # TODO: make a smooth transition recommendation
            # Checking current O2 level
            q = f"""SELECT raw.f_value FROM {_DB_NAME_}.cb_display disp
                    LEFT JOIN {_DB_NAME_}.tb_bat_raw raw
                    ON disp.f_tags = raw.f_address_no 
                    WHERE disp.f_desc = "excess_o2" """
            current_oxygen = pd.read_sql(q, con).values[0][0]
            # Latest recommendation
            q = f"""SELECT gen.model_id, gen.ts, conf.f_tag_name, conf.f_description, 
                    gen.value, gen.bias_value, gen.enable_status, gen.value - gen.bias_value AS 'current_value' 
                    FROM {_DB_NAME_}.tb_tags_read_conf conf
                    LEFT JOIN {_DB_NAME_}.tb_combustion_model_generation gen
                    ON conf.f_description = gen.tag_name 
                    WHERE f_category = "Recommendation"
                    AND gen.ts = (SELECT MAX(ts) FROM {_DB_NAME_}.tb_combustion_model_generation tcmg)"""
            Recom = pd.read_sql(q, con)
            set_point_oxygen = Recom[Recom['f_description'] == 'Excess Oxygen Sensor']['value']
            if (abs(set_point_oxygen - current_oxygen) < config.OXYGEN_STEADY_STATE_LEVEL): 
                return {'message': 'Oxygen is in steady state level.'}
            
            # Write recommendation to OPC
            bg_write_recommendation_to_opc(MAX_BIAS_PERCENTAGE)
            return {'message':f"Waiting to next {LATEST_RECOMMENDATION_TIME + pd.Timedelta(f'{RECOM_EXEC_INTERVAL}min') - now} min"}
        
        else:
            # Calling ML Recommendations to the latest recommendation
            ML = bg_get_ml_recommendation()

            if type(ML) is not dict: 
                return {'message':'Error on ML response. Columns "model_status" not found.'}

            if ML['model_status'] == 1:
                try:
                    bg_write_recommendation_to_opc(MAX_BIAS_PERCENTAGE)
                except Exception as e:
                    return {'message': str(e)}
                return {'message':'Done!'}

if _LOCAL_MODE_:
    k = bg_ml_runner()
    print(time.strftime('%X\t'), k)
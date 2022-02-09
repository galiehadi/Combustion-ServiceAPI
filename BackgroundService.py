import pandas as pd
import numpy as np
import time, db_config, sqlalchemy, requests
from urllib.parse import quote_plus as urlparse
from pprint import pprint

_UNIT_CODE_ = db_config._UNIT_CODE_
_UNIT_NAME_ = db_config._UNIT_NAME_
_USER_ = db_config._USER_
_PASS_ = urlparse(db_config._PASS_)
_IP_ = db_config._IP_
_DB_NAME_ = db_config._DB_NAME_
_LOCAL_IP_ = db_config._LOCAL_IP_

con = f"mysql+mysqlconnector://{_USER_}:{_PASS_}@{_IP_}/{_DB_NAME_}"

def bg_safeguard_check():
    t0 = time.time()
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
                hdr.f_rule_descr = "SAFEGUARD"
            ORDER BY
                rule.f_sequence"""
    df = pd.read_sql(q, con)
    
    sg = df[['f_bracket_open','f_value','f_bracket_close']]

    Safeguard_status = True
    Safeguard_text = ''
    for i in sg.index:
        bracketOpen, value, bracketClose = sg.iloc[i]
        Safeguard_text += f"{bracketOpen}{value}{bracketClose} "

    Safeguard_text = Safeguard_text.lower()
    Safeguard_status = eval(Safeguard_text)

    ret = {
        'Safeguard Status': Safeguard_status,
        'Execution time': str(round(time.time() - t0,3)) + ' sec'
    }
    return ret

def bg_safeguard_update():
    ret = bg_safeguard_check()
    Safeguard_status = ret['Safeguard Status']
    
    engine = sqlalchemy.create_engine(con)

    q = f"""UPDATE {_DB_NAME_}.tb_bat_raw SET f_date_rec=NOW(), f_value={1 if Safeguard_status else 0}, f_updated_at=NOW()
            WHERE f_address_no = "{db_config.SAFEGUARD_TAG}" """

    with engine.connect() as conn:
        res = conn.execute(q)
    return ret

def bg_get_recom_exec_interval():
    q = f"""SELECT f_default_value FROM {_DB_NAME_}.tb_combustion_parameters tcp 
            WHERE f_label = 'RECOM_EXEC_INTERVAL' """
    df = pd.read_sql(q, con)
    recom_exec_interval = float(df.values)
    return recom_exec_interval

def bg_machine_learning_update():
    response = requests.get(f'http://{_LOCAL_IP_}:5002/bat_combustion/RBG1/realtime')
    ret = response.text
    return ret

def bg_get_ml_recommendation():
    try:
        response = requests.get(f'http://{_LOCAL_IP_}:5002/bat_combustion/RBG1/realtime')
        ret = response.text
        return ret
    except Exception as e:
        print(time.ctime(),'- Machine learning prediction error:', e)
        return str(e)

def bg_ml_runner():
    # Get CombustionEnable status
    q = f"""SELECT raw.f_value FROM {_DB_NAME_}.tb_combustion_parameters param 
            LEFT JOIN {_DB_NAME_}.tb_bat_raw_history raw
            ON param.f_default_value = raw.f_address_no 
            WHERE param.f_label = "TAG_ENABLE_COPT"
            ORDER BY raw.f_date_rec DESC LIMIT 2 """
    df = pd.read_sql(q, con)
    ENABLE_COPT, LAST_ENABLE_COPT = df.T.astype(bool).values[0]
    FIRST_ENABLED = 1 if (not LAST_ENABLE_COPT and ENABLE_COPT) else 0
    MAX_BIAS_PERCENTAGE = 5
    OPC_COLUMNS = 'bias_value'

    # Harusnya if ENABLE_COPT
    if not ENABLE_COPT:
        q = f"""SELECT gen.model_id, gen.ts, conf.f_tag_name, gen.bias_value, 
                gen.value, gen.value - gen.bias_value AS original_value 
                FROM {_DB_NAME_}.tb_combustion_model_generation gen
                LEFT JOIN {_DB_NAME_}.tb_tags_read_conf conf
                ON gen.tag_name = conf.f_description 
                WHERE gen.ts = (SELECT MAX(ts) FROM {_DB_NAME_}.tb_combustion_model_generation)"""
        df = pd.read_sql(q, con)
        
        # TODO: Check latest ts OPC
        
        # #

        q = f"""SELECT f_default_value FROM db_bat_rmb2.tb_combustion_parameters tcp 
                WHERE f_label IN ("MAX_BIAS_PERCENTAGE","SEND_VALUE_TO_OPC") """

        try: MAX_BIAS_PERCENTAGE, OPC_COLUMNS = float(pd.read_sql(q, con).T.values[0])
        except: pass
        
        for i in df.index:
            df.loc[i, 'bias_value'] = max((0-MAX_BIAS_PERCENTAGE*0.01) * df.loc[i, 'original_value'], df.loc[i, 'bias_value'])
            df.loc[i, 'bias_value'] = min((0+MAX_BIAS_PERCENTAGE*0.01) * df.loc[i, 'original_value'], df.loc[i, 'bias_value'])
        df['value'] = df['original_value'] + df['bias_value']
        
        opc_write = df[['f_tag_name','ts',OPC_COLUMNS]]
        opc_write.columns = ['tag_name','ts','value']

        opc_write.to_sql('tb_opc_write', con=con, if_exists='append', index=False)
        opc_write.to_sql('tb_opc_write_history', con=con, if_exists='append', index=False)
        

    return ENABLE_COPT, LAST_ENABLE_COPT


print(bg_ml_runner())
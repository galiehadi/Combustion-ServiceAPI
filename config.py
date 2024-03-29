"""
Setting parameter untuk koneksi ke database.
"""
__version__ = 'v1.5.6'

_UNIT_CODE_ = "PCT2"
_UNIT_NAME_ = "PLTU Pacitan 2"
_USER_ = "bat_copt"
_PASS_ = "P@ssw0rd"
_IP_ = "192.168.32.7:3306" # "10.7.1.116:33009" # 
_LOCAL_IP_ = "192.168.31.97:5002" # "192.168.32.7:5002" # "0.0.0.0:5002"
_DB_NAME_ = "db_bat_pct2"

# "Setting parameter nama variabel"

WATCHDOG_TAG = "OPC.AW2002.2LSB.WATCHDOG_STS.BO02"
SAFEGUARD_TAG = "SAFEGUARD:COMBUSTION"
SAFEGUARD_SOPT_TAG = "SAFEGUARD:SOOTBLOW"
SAFEGUARD_USING_MAX_VIOLATED = False

DESC_ENABLE_COPT = "COPT ENABLE STATUS"
DESC_ENABLE_COPT_BT = "BURN TILT ENABLE"
DESC_ENABLE_COPT_SEC = "SEC AIR ENABLE"
DESC_ENABLE_COPT_MOT = "MILL OUTLET ENABLE"
DESC_ALARM = "SAFEGUARD FAIL ALARM"

TAG_COPT_ISCALLING = "TAG:COPT_is_calling"
OXYGEN_STEADY_STATE_LEVEL = 0.01

PARAMETER_BIAS = 'bias_value'
PARAMETER_SET_POINT = 'value'

PARAMETER_WRITE = {
    'All Wind': PARAMETER_BIAS,
    'Burner Tilt Position Lower': PARAMETER_BIAS,
    'Burner Tilt Position Upper': PARAMETER_BIAS,
    'Excess Oxygen Sensor': PARAMETER_BIAS,
    'Mill A Outlet Temperature': PARAMETER_SET_POINT,
    'Mill B Outlet Temperature': PARAMETER_SET_POINT,
    'Mill C Outlet Temperature': PARAMETER_SET_POINT,
    'Mill D Outlet Temperature': PARAMETER_SET_POINT,
    'Mill E Outlet Temperature': PARAMETER_SET_POINT,
    'Mill F Outlet Temperature': PARAMETER_SET_POINT,
    'Total Secondary Air Flow': PARAMETER_BIAS
}

REALTIME_OPC_TRANSFER_TAG = {
    'Efficiency': {
        'tb_bat_raw_tag': 'Efficiency',
        'opc_tag':'OPC.AW1002.1E_FDF.COPT_1.RI03'
    },
    'Efficiency Baseline': {
        'tb_bat_raw_tag': 'Eff_Baseline',
        'opc_tag':'OPC.AW1002.1E_FDF.COPT_1.RI02'
    }
}

TEMP_FOLDER = 'data/temp/'

"Setting Karakter O2 di DCS berdasarkan Steam Flow"

# DCS_Xp = [ 0,   45,   77,   91,  100 ] # Dalam persen
DCS_X  = [ 0, 400, 600, 800, 1025, 1200 ]
DCS_Y  = [ 10, 6, 4.5, 3.5, 3, 2.5 ]

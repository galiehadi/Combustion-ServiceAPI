"""
Setting parameter untuk koneksi ke database.
"""

_UNIT_CODE_ = "TAA1"
_UNIT_NAME_ = "PLTU Tanjung Awar-Awar Unit 1"
_USER_ = "root"
_PASS_ = "P@ssw0rd"
_IP_ = "192.168.1.10:3306" # "10.7.1.116:33002" # 
_LOCAL_IP_ = "192.168.1.10"
_DB_NAME_ = "db_bat_tja1"

"Setting parameter nama variabel"

WATCHDOG_TAG = "WatchdogStatus"
SAFEGUARD_TAG = "SAFEGUARD:COMBUSTION"
SAFEGUARD_SOPT_TAG = "SAFEGUARD:SOOTBLOW"

DESC_ENABLE_COPT = "COMBUSTION ENABLE"
DESC_ENABLE_COPT_BT = "BURN TILT ENABLE"
DESC_ENABLE_COPT_SEC = "SEC AIR ENABLE"
DESC_ENABLE_COPT_MOT = "MILL OUTLET ENABLE"
DESC_ALARM = "Combustion Alarm"

TAG_COPT_ISCALLING = "TAG:COPT_is_calling"
OXYGEN_STEADY_STATE_LEVEL = 0.01

PARAMETER_BIAS = 'bias_value'
PARAMETER_SET_POINT = 'value'

PARAMETER_WRITE = {
    'All Wind': PARAMETER_BIAS,
    'Burner Tilt Position 0': PARAMETER_BIAS,
    'Burner Tilt Position 1': PARAMETER_BIAS,
    'Burner Tilt Position 2': PARAMETER_BIAS,
    'Burner Tilt Position 3': PARAMETER_BIAS,
    'Excess O2': PARAMETER_SET_POINT,
    'Mill A Outlet Temperature': PARAMETER_SET_POINT,
    'Mill B Outlet Temperature': PARAMETER_SET_POINT,
    'Mill C Outlet Temperature': PARAMETER_SET_POINT,
    'Mill D Outlet Temperature': PARAMETER_SET_POINT,
    'Mill E Outlet Temperature': PARAMETER_SET_POINT,
    'Mill F Outlet Temperature': PARAMETER_SET_POINT,
    'Total Secondary Air Flow': PARAMETER_BIAS
}

TEMP_FOLDER = 'data/temp/'
"""
Setting parameter untuk koneksi ke database.
"""
__version__ = 'v1.5'

_UNIT_CODE_ = "RBG2"
_UNIT_NAME_ = "PLTU Rembang Unit 1"
_USER_ = "bat_copt"
_PASS_ = "P@ssw0rd"
_IP_ = "10.7.1.116:33011" # "35.219.48.62" # 
_LOCAL_IP_ = "localhost:5002" # "0.0.0.0:5002"
_DB_NAME_ = "db_bat_rmb1"

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
    'Burner Tilt Position 1L': PARAMETER_BIAS,
    'Burner Tilt Position 1R': PARAMETER_BIAS,
    'Burner Tilt Position 2L': PARAMETER_BIAS,
    'Burner Tilt Position 2R': PARAMETER_BIAS,
    'Burner Tilt Position 3L': PARAMETER_BIAS,
    'Burner Tilt Position 3R': PARAMETER_BIAS,
    'Burner Tilt Position 4L': PARAMETER_BIAS,
    'Burner Tilt Position 4R': PARAMETER_BIAS,
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

"Setting Karakter O2 di DCS"
DCS_Xp = [ 0,   45,   77,   91,  100] # Dalam persen
DCS_X  = [ 0,  150,  255,  300,  330]
DCS_Y  = [ 8,    6,  4.5,    4,    4]
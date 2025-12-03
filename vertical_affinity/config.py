"""
Configuration file for database connections and constants.
"""
from urllib.parse import quote_plus
from datetime import datetime, timedelta
import pandas as pd

# ======================== DATABASE CONFIGURATION ========================

# MySQL Database Configuration
DB_HOST = 'rm-bp1o6283vw74wr4x5.mysql.rds.aliyuncs.com'
DB_PORT = 3306
DB_USER = 'on_ds_test'
DB_PASS = quote_plus('HC73aA7aeLxJFLFH_@bPeHkv')
DB_NAME = 'on_ds_test'

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"

# Trino Database Configuration
TRINO_HOST = '192.168.0.163'
TRINO_PORT = 8082
TRINO_USER = 'ta'
TRINO_CATALOG = 'hive'
TRINO_SCHEMA = 'ta'

# ======================== TABLE NAMES ========================

PRODUCT_MYSQL = 'dim_product_colors_info'
EMPLOYEE_MYSQL = 'stg_employee'
MEMBER_MYSQL = 'dwd_crm_members'

# ======================== DATE CONFIGURATION ========================

CURRENT_DATE = pd.to_datetime(datetime.now().date())
CURRENT_DATE_SQL = f"'{CURRENT_DATE.strftime('%Y-%m-%d')}'"
R4M_DATE = CURRENT_DATE - pd.DateOffset(months=4)
R4M_DATE_SQL = f"'{R4M_DATE.strftime('%Y-%m-%d')}'"
R10M_DATE = CURRENT_DATE - pd.DateOffset(months=10)
R10M_DATE_SQL = f"'{R10M_DATE.strftime('%Y-%m-%d')}'"

# ======================== VERTICAL CONFIGURATION ========================

REQUIRED_VERTICALS = ['running', 'training', 'outdoor', 'allday', 'tennis']
TARGET_VERTICALS = ['tennis', 'running', 'outdoor', 'training', 'allday']

# Vertical mapping dictionary
VERTICAL_MAPPING = {
    'Performance All Day': 'allday',
    'PAD': 'allday',
    'Performance Running': 'running',
    'PR': 'running',
    'Performance Training ': 'training',
    'PTR': 'training',
    'Performance Outdoor': 'outdoor',
    'PO': 'outdoor',
    'Performance Tennis': 'tennis',
    'PT': 'tennis'
}

# ======================== EVENT CONFIGURATION ========================

EVENT_PDP_VIEW = 'pdp_view'
EVENT_ADD_TO_CART = 'add_to_cart'
BASE_COLUMNS = ['account_id', 'vertical']

# ======================== FEATURE WEIGHTS ========================

DIMENSION_WEIGHTS_TEMPLATE = {
    'R': 0.1,
    'F_4m': 0.2,
    'F_1y': 0.1,
    'M_4m': 0.2,
    'M_1y': 0.15,
    'PDP_View_6M_Count': 0.05,
    'PDP_View_Days_Since_Last': 0.05,
    'ATC_6M_Count': 0.05,
    'ATC_Days_Since_Last': 0.05,
    'activity_count': 0.00,
    'navi_6M_count': 0.05,
}

# ======================== PANDAS DISPLAY OPTIONS ========================

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)


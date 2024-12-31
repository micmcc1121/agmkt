'''
Creates dictionaries to create a standard library of
crop charts from the NASS QuickStats SQL database.

Created by: Micahel McConnell
Created on: December 30, 2024
'''

# Libraries
import agmkt_pkg.nass as nass
import agmkt_pkg.env_vars as env_vars
from sqlalchemy import create_engine
import psycopg2

import matplotlib.pyplot as plt
plt.style.use('./style_sheets/agmktstyle.mplstyle')

# Connect to Database

user = env_vars.mktdb_user()
password = env_vars.mktdb_passwd()
server = env_vars.mktdb_server()

connection_string = f'postgresql://{user}:{password}@{server}/mktdb'
eng = create_engine(connection_string)
conn = eng.connect()

# Dictionaries

# Corn

stat_list = [#('AREA PLANTED', 'ACRES', 'Million'), 
             ('AREA HARVESTED', 'ACRES', 'Million'), 
             ('YIELD', 'BU / ACRE', 'Unit'), 
             ('PRODUCTION', 'BU', 'Million'), 
             ('PRODUCTION', '$', 'Billion'), 
             ('PRICE RECEIVED', '$ / BU', 'Unit')]

for s, u, unit in stat_list:
    stmt_dict = {'COMMODITY_DESC':'CORN', 
                 'PRODN_PRACTICE_DESC':'ALL PRODUCTION PRACTICES', 
                 'UTIL_PRACTICE_DESC':'GRAIN', 
                 'STATISTICCAT_DESC':s, 
                 'UNIT_DESC':u, 
                 'AGG_LEVEL_DESC':'NATIONAL', 
                 'REFERENCE_PERIOD_DESC':'YEAR'}
    df = nass.df_time_series_chart(stmt_dict, conn, 'nass_crops_fmt')
    nass.time_series_chart(df, stmt_dict, './docs', years=40, scale=unit)



# Soybeans

# Hay

# Sorghum

# Barley

# Oats

# Wheat

# Canola

# Sunflower
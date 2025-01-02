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

# Chart Repository Folder

chart_path = './docs'

# Dictionaries

# Corn

stat_list = [#('AREA PLANTED', 'ACRES', 'Million'), 
             ('AREA HARVESTED', 'ACRES', 'YEAR', 'Million'), 
             ('YIELD', 'BU / ACRE', 'YEAR', 'Unit'), 
             ('PRODUCTION', 'BU', 'YEAR', 'Million'), 
             ('PRODUCTION', '$', 'YEAR', 'Billion'), 
             ('PRICE RECEIVED', '$ / BU', 'MARKETING YEAR', 'Unit')]

for s, u, f, unit in stat_list:
    stmt_dict = {'COMMODITY_DESC':'CORN', 
                 'PRODN_PRACTICE_DESC':'ALL PRODUCTION PRACTICES', 
                 'UTIL_PRACTICE_DESC':'GRAIN', 
                 'STATISTICCAT_DESC':s, 
                 'UNIT_DESC':u, 
                 'AGG_LEVEL_DESC':'NATIONAL', 
                 'REFERENCE_PERIOD_DESC':f}
    df = nass.df_time_series_chart(stmt_dict, conn, 'nass_crops_fmt')
    nass.time_series_chart(df, stmt_dict, chart_path, years=40, scale=unit)

# Soybeans

stat_list = [('AREA PLANTED', 'ACRES', 'YEAR', 'Million'), 
             ('AREA HARVESTED', 'ACRES', 'YEAR', 'Million'), 
             ('YIELD', 'BU / ACRE', 'YEAR', 'Unit'), 
             ('PRODUCTION', 'BU', 'YEAR', 'Million'), 
             ('PRODUCTION', '$', 'YEAR', 'Billion'), 
             ('PRICE RECEIVED', '$ / BU', 'MARKETING YEAR', 'Unit')]

for s, u, f, unit in stat_list:
    stmt_dict = {'COMMODITY_DESC':'SOYBEANS', 
                 'PRODN_PRACTICE_DESC':'ALL PRODUCTION PRACTICES', 
                 'STATISTICCAT_DESC':s, 
                 'UNIT_DESC':u, 
                 'AGG_LEVEL_DESC':'NATIONAL', 
                 'REFERENCE_PERIOD_DESC':f}
    df = nass.df_time_series_chart(stmt_dict, conn, 'nass_crops_fmt')
    nass.time_series_chart(df, stmt_dict, chart_path, years=40, scale=unit)

# Hay

class_list = ['ALL CLASSES', 'ALFALFA']

for c in class_list:

    stat_list = [('AREA HARVESTED', 'ACRES', 'YEAR', 'Million'), 
                 ('YIELD', 'TONS / ACRE', 'YEAR', 'Unit'), 
                 ('PRODUCTION', 'TONS', 'YEAR', 'Million'), 
                 ('PRODUCTION', '$', 'YEAR', 'Billion'), 
                 ('PRICE RECEIVED', '$ / TON', 'MARKETING YEAR', 'Unit')]

    for s, u, f, unit in stat_list:
        stmt_dict = {'COMMODITY_DESC':'HAY', 
                     'CLASS_DESC':c,
                     'PRODN_PRACTICE_DESC':'ALL PRODUCTION PRACTICES',  
                     'STATISTICCAT_DESC':s, 
                     'UNIT_DESC':u, 
                     'AGG_LEVEL_DESC':'NATIONAL', 
                     'REFERENCE_PERIOD_DESC':f}
        df = nass.df_time_series_chart(stmt_dict, conn, 'nass_crops_fmt')
        nass.time_series_chart(df, stmt_dict, chart_path, years=40, scale=unit)

# Sorghum

stat_list = [#('AREA PLANTED', 'ACRES', 'Million'), 
             ('AREA HARVESTED', 'ACRES', 'YEAR', 'Million'), 
             ('YIELD', 'BU / ACRE', 'YEAR', 'Unit'), 
             ('PRODUCTION', 'BU', 'YEAR', 'Million'), 
             ('PRODUCTION', '$', 'YEAR', 'Billion'), 
             ('PRICE RECEIVED', '$ / CWT', 'MARKETING YEAR', 'Unit')]

for s, u, f, unit in stat_list:
    stmt_dict = {'COMMODITY_DESC':'SORGHUM', 
                 'PRODN_PRACTICE_DESC':'ALL PRODUCTION PRACTICES', 
                 'UTIL_PRACTICE_DESC':'GRAIN', 
                 'STATISTICCAT_DESC':s, 
                 'UNIT_DESC':u, 
                 'AGG_LEVEL_DESC':'NATIONAL', 
                 'REFERENCE_PERIOD_DESC':f}
    df = nass.df_time_series_chart(stmt_dict, conn, 'nass_crops_fmt')
    nass.time_series_chart(df, stmt_dict, chart_path, years=40, scale=unit)

# Barley

stat_list = [#('AREA PLANTED', 'ACRES', 'Million'), 
             ('AREA HARVESTED', 'ACRES', 'YEAR', 'Million'), 
             ('YIELD', 'BU / ACRE', 'YEAR', 'Unit'), 
             ('PRODUCTION', 'BU', 'YEAR', 'Million'), 
             ('PRODUCTION', '$', 'YEAR', 'Billion'), 
             ('PRICE RECEIVED', '$ / BU', 'MARKETING YEAR', 'Unit')]

for s, u, f, unit in stat_list:
    stmt_dict = {'COMMODITY_DESC':'BARLEY', 
                 'CLASS_DESC':'ALL CLASSES',
                 'PRODN_PRACTICE_DESC':'ALL PRODUCTION PRACTICES',  
                 'STATISTICCAT_DESC':s, 
                 'UNIT_DESC':u, 
                 'AGG_LEVEL_DESC':'NATIONAL', 
                 'REFERENCE_PERIOD_DESC':f}
    df = nass.df_time_series_chart(stmt_dict, conn, 'nass_crops_fmt')
    nass.time_series_chart(df, stmt_dict, chart_path, years=40, scale=unit)

# Oats

stat_list = [('AREA PLANTED', 'ACRES', 'YEAR', 'Million'), 
             ('AREA HARVESTED', 'ACRES', 'YEAR', 'Million'), 
             ('YIELD', 'BU / ACRE', 'YEAR', 'Unit'), 
             ('PRODUCTION', 'BU', 'YEAR', 'Million'), 
             ('PRODUCTION', '$', 'YEAR', 'Billion'), 
             ('PRICE RECEIVED', '$ / BU', 'MARKETING YEAR', 'Unit')]

for s, u, unit in stat_list:
    stmt_dict = {'COMMODITY_DESC':'OATS', 
                 'PRODN_PRACTICE_DESC':'ALL PRODUCTION PRACTICES',  
                 'STATISTICCAT_DESC':s, 
                 'UNIT_DESC':u, 
                 'AGG_LEVEL_DESC':'NATIONAL', 
                 'REFERENCE_PERIOD_DESC':f}
    df = nass.df_time_series_chart(stmt_dict, conn, 'nass_crops_fmt')
    nass.time_series_chart(df, stmt_dict, chart_path, years=40, scale=unit)

# Wheat

# Canola

stat_list = [('AREA PLANTED', 'ACRES', 'YEAR', 'Million'), 
             ('AREA HARVESTED', 'ACRES', 'YEAR', 'Million'), 
             ('YIELD', 'LB / ACRE', 'YEAR', 'Unit'), 
             ('PRODUCTION', 'LB', 'YEAR', 'Million'), 
             ('PRODUCTION', '$', 'YEAR', 'Billion'), ]

for s, u, f, unit in stat_list:
    stmt_dict = {'COMMODITY_DESC':'CANOLA', 
                 'PRODN_PRACTICE_DESC':'ALL PRODUCTION PRACTICES', 
                 'STATISTICCAT_DESC':s, 
                 'UNIT_DESC':u, 
                 'AGG_LEVEL_DESC':'NATIONAL', 
                 'REFERENCE_PERIOD_DESC':f}
    df = nass.df_time_series_chart(stmt_dict, conn, 'nass_crops_fmt')
    nass.time_series_chart(df, stmt_dict, chart_path, years=40, scale=unit)

# Sunflower
class_list = ['ALL CLASSES']

for c in class_list:
    stat_list = [('AREA PLANTED', 'ACRES', 'YEAR', 'Million'), 
                 ('AREA HARVESTED', 'ACRES', 'YEAR', 'Million'), 
                 ('YIELD', 'LB / ACRE', 'YEAR', 'Unit'), 
                 ('PRODUCTION', 'LB', 'YEAR', 'Million'), 
                 ('PRODUCTION', '$', 'YEAR', 'Billion'), 
                 ('PRICE RECEIVED', '$ / CWT', 'MARKETING YEAR', 'Unit')]

    for s, u, f, unit in stat_list:
        stmt_dict = {'COMMODITY_DESC':'SUNFLOWER', 
                     'CLASS_DESC': c,
                     'PRODN_PRACTICE_DESC':'ALL PRODUCTION PRACTICES', 
                     'STATISTICCAT_DESC':s, 
                     'UNIT_DESC':u, 
                     'AGG_LEVEL_DESC':'NATIONAL', 
                     'REFERENCE_PERIOD_DESC':f}
        df = nass.df_time_series_chart(stmt_dict, conn, 'nass_crops_fmt')
        nass.time_series_chart(df, stmt_dict, chart_path, years=40, scale=unit)

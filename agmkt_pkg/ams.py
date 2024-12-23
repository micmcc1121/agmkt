'''
Package focusing on functions and processes
related to ETL of AMS data.
'''

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import agmkt_pkg.utils as utils

def df_grain_time_series_chart(stmt_dict, db_connect, table='mars_grain_bids'):
    """Creates the DataFrame for charts from AMS SQL database derived from MARS API

    Args:
        stmt_dict (dict): _description_
        db_connect (SQLAlchemy create_engine): _description_
        table (string): table name of data origin

    Returns:
        pandas.DataFrame: Pandas DataFrame structured for time series charts
    """
    filter_string = utils.sql_filter_string(stmt_dict)

    select_stmt = f'''
    SELECT *
    FROM {table}
    WHERE report_date is not NULL
    '''
    select_stmt += filter_string
    select_stmt += ';'

    df = pd.read_sql(select_stmt, con = db_connect)
    df = df.set_index('report_date')
    df['basis Mid'] = (df['basis Max'] + df['basis Min']/2)
    
    return df
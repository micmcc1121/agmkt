'''
Package focusing on functions and processes
related to ETL of NASS data.
'''

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import agmkt_pkg.utils as utils

def cattle_expansion_vars(df):
    """Calculates variables relating to inventory cycles.  Creates a datetime index to anchor the time series.

    Args:
        df (pandas.DataFrame): Pandas DataFrame with NASS QS schema strcuture from mktdb

    Returns:
        pandas.DataFrame: returns a Pandas DataFrame with the augmented columns
    """   
    try:
        df0 = df
        df0['date'] = pd.to_datetime(df0['YEAR'].astype('str') + '-' + df0['BEGIN_CODE'].astype('str') + '-01', format='%Y-%m-%d')
        df0 = df0.set_index('date')
        df0 = df0.assign(
            expansion = np.where(df0['VALUE'] > df0['VALUE'].shift(1), True, False),
            cycle_start = np.where((df0['VALUE'] > df0['VALUE'].shift(1)) & (df0['VALUE'].shift(1) < df0['VALUE'].shift(2)), True, False),
            cycle_peak = np.where((df0['VALUE'] < df0['VALUE'].shift(1)) & (df0['VALUE'].shift(1) > df0['VALUE'].shift(2)), True, False),
        )
    except:
        print('DataFrame ETL error')
    return df0

def df_time_series_chart(stmt_dict, db_connect, table='nass_crops_fmt'):
    """Calculates variables relating to inventory cycles.  Creates a datetime index to anchor the time series.

    Args:
        stmt_dict (dictionary): dictionary with column names as keys and filter values as values.
        db_connect (SQLAlchemy create_engine connection): SQL database connection
        table (string): name of Table in the SELECT statement for the Postgresql database connection

    Returns:
        matplotlib object and png image: returns a matplotlib object and png file
    """   
    
    filter_string = utils.sql_filter_string(stmt_dict)

    select_stmt = f'''
    SELECT *
    FROM {table}
    WHERE "SOURCE_DESC" IN ('SURVEY')
    '''
    select_stmt += filter_string
    select_stmt += ';'

    df = pd.read_sql(select_stmt, con = db_connect)
    
    df['date'] = df['YEAR'].astype('str') + '-01-01' #Need to generalize for monthly and quartelry data
    df['date'] = pd.to_datetime(df['date'])
    # Make datetime index once date variable is generalized

    return df
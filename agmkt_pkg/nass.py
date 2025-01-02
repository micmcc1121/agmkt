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
    """Created dataframes filtering data from NASS QS SQL database

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

def time_series_chart(df, dict, path, years=50, scale='Unit'):
    """_summary_

    Args:
        df (_type_): _description_
        scale (str, optional): _description_. Defaults to 'Unit'.
    """
    scale_dict = {
        'Unit':{'value':1, 'label':''},
        'Thousand':{'value':1e3, 'label':'Thousand'},
        'Million':{'value':1e6, 'label':'Million'},
        'Billion':{'value':1e9, 'label':'Billion'},
    }

    df_viz = df[df['date'].dt.year >= df['date'].dt.year.max() - years]

    dict = dict

    str_commodity = dict['COMMODITY_DESC'].title()
    str_statistic = dict['STATISTICCAT_DESC'].title()
    str_unit = dict['UNIT_DESC'].title()

    fig = plt.subplot()
    scale = scale
    scale_value = scale_dict[scale]['value']
    scale_label = scale_dict[scale]['label']
    sns.lineplot(data=df_viz, x='date', y=df['VALUE']/scale_value)
    sns.scatterplot(data=df_viz, x='date', y=df['VALUE']/scale_value)
    plt.title(f'U.S. {str_commodity} {str_statistic}')
    plt.ylabel(f'{scale_label} {str_unit}')
    plt.xlabel('Crop Year')

    # Construct filename using select statement dictionary

    keys = ['COMMODITY_DESC', 'CLASS_DESC', 'UTIL_PRACTICE_DESC', 'STATISTICCAT_DESC', 'UNIT_DESC', 'REFERENCE_PERIOD_DESC', 'STATE_ALPHA']

    filename = ''

    for key in keys:
        if dict.get(key) == None:
            next
        else:
            filename += dict.get(key).lower() + '_'

    filename = filename[:-1] + '.png'
    filename = filename.replace(' ', '').replace('$', 'dollars').replace('/', 'per').lower() # replace any characters that would interfer with file path

    plt.savefig(f'{path}/{filename}')
    plt.clf()

    return print(f'Matplot chart saved to {path}/{filename}')
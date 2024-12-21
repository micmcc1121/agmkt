'''
Package focusing on functions and processes
related to ETL of NASS data.
'''

import pandas as pd
import numpy as np

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
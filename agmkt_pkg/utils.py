'''
General utility functions to 
be used across project.
'''

def sql_filter_string(dict):
    """Uses a dictionary key-value pair to create a string that can be inserted into a SELECT statement's WHERE clause.

    Args:
        dict (dictionary): dictionary with column names as keys and filter values as values.
        
    Returns:
        filter_string (string): string that can be inserted into a SELECT statement.
    """   
    
    filter_string = ''

    for key, value in dict.items():
        try:
            if type(dict.get(key)) == str:
                filter_string += f"""AND "{key}" IN ('{value}') """
            elif type(dict.get(key)) == list:
                list_str = ""
                for v in value:
                    list_str += f"""'{v}', """
                list_str = list_str[:-2]
                filter_string += f"""AND "{key}" IN ({list_str}) """
            else:
                TypeError("Dictionary value type no found for Filter String concatenation")
        except: 
            pass # Do not leave as a pass
    return filter_string

def keep_cols(df, default_cols):
    """Removes columns from a Pandas dataframe that does not have unique values

    Args:
        df (pandas DataFrame): Input dataframe
        default_cols (list): List of columns that are to be included regardless of unique values

    Returns:
        pandas.DataFrame: processed DataFrame that has any extra columns removed
    """
    cols = []

    if len(default_cols) >= 1:
        for i in default_cols:
            cols.append(i)
    else:
        next

    for col in df.columns:
        if df[col].nunique() > 1:
            cols.append(col)
    
    df0 = df[cols]

    return df0


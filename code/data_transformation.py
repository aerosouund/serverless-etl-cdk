def join_dfs(df, jh, col='Date'):
    ''' Takes in two dataframes and preforms a left join
    that happens on the Date column, change the col argument
    for a different column'''
    final = df.merge(jh, on=col, how='left')
    return final


def change_to_datetime(df, col='Date'):
    ''' Changes a column in a dataframe to type datetime'''
    df[col] = df[col].astype('datetime64[ns]')
    return

def rename_cols(df, names_dict):
    ''' Renames a group of columns in a df by passing a dict of old names and new names  '''
    df.rename(columns=names_dict, inplace=True)
    return


def clean_dataframe(jh, country, columns, col='Country/Region'):
    ''' Filters a dataframe column from entries that don't
    match a specific condition and removes needless columns,
    used for cleaning the John Hopkins data'''
    jh.drop(jh[jh[col] != country].index, inplace=True)
    jh.drop(columns, axis=1, inplace=True)
    return

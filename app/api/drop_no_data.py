"""
The functions in this file relate to dopping rows that have no data in the columns 
for cumulative and outbreak data.
"""
from app.api import utils

def drop_no_data(df):
    # columns with covid data
    col_map = utils.make_matching_column_name_map(df)
    data_cols = [*col_map.keys()] + [*col_map.values()]
    df = df.dropna(how='all', subset=data_cols)
    return df

"""
The functions in this file relate to filling in cumulative data in which 
there is "NO DATA".
"""

from datetime import date, timedelta

import flask
import numpy as np
import pandas as pd

from app.api import utils

_NO_DATA = {'NO DATA', 'Not Reported', 'Data Pending', 'Closed'}

def replace_no_data(df):
    cume_cols = [x for x in df.columns if x.startswith('Cume_')]
    
    # sort the no-data rows in chronological order so we fix them from the bottom up
    no_data = df[df[cume_cols].isin(_NO_DATA).any(1)].sort_values('Date')

    for index, row in no_data.iterrows():
        facility = df.loc[df.ctp_id == row.ctp_id]

        prev_dates = [x for x in set(facility.Date) if x < row.Date]
        if len(prev_dates) == 0:
            flask.current_app.logger.info('First outbreak date for %s contains a "no data" string, clearing it...' % row.Facility)
            for col in cume_cols:
                if row[col] in _NO_DATA:
                    df.loc[index, col] = np.nan
            continue

        most_recent_date = max(prev_dates)

        prev_row = df.loc[
            (df.Date == most_recent_date) &
            (df.ctp_id == row.ctp_id)].iloc[0]

        for col in cume_cols:
            if row[col] in _NO_DATA:
                df.loc[index, col] = prev_row[col]

    return df

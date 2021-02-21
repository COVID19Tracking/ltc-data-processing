"""
The functions in this file relate to filling in missing data for a set of dates when 
"""

from datetime import date, timedelta

import flask
import numpy as np
import pandas as pd
from time import time

from app.api import utils


def fill_missing_dates(df):
    starting_date = date(2020, 5, 21)
    collection_dates = []
    while starting_date <= date.today():
        collection_dates.append(int(starting_date.strftime('%Y%m%d')))
        starting_date = starting_date + timedelta(days=7)

    collection_dates = [x for x in collection_dates if x >= min(df.Date)]

    missing_dates_dfs = []
    for collection_date in collection_dates:
        # is there any data for this date?
        existing_data = df.loc[df.Date == collection_date]
        if existing_data.empty:
            print('Missing data for %d' % collection_date)

            # get the most recent date that there is at least one facility for
            most_recent_date = max(x for x in set(df.Date) if x < collection_date)
            data = df.loc[df.Date == most_recent_date].copy()
            data['Date'] = collection_date
            missing_dates_dfs.append(data)

    processed_df = pd.concat([df, *missing_dates_dfs])
    processed_df.sort_values(
        by=['Facility', 'County', 'City', 'Date'], ignore_index=True, inplace=True)

    return processed_df

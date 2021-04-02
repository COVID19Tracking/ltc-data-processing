"""
The functions in this file relate to filling in missing data 
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
    while starting_date <= date(2021, 3, 4):
        collection_dates.append(int(starting_date.strftime('%Y%m%d')))
        starting_date = starting_date + timedelta(days=7)

    collection_dates = [x for x in collection_dates if x >= min(df.Date)]

    missing_dates_dfs = []
    for collection_date in collection_dates:
        # is there any data for this date?
        existing_data = df.loc[df.Date == collection_date]
        if existing_data.empty:
            # get the most recent date that there is at least one facility for
            most_recent_date = max(x for x in set(df.Date) if x < collection_date)
            flask.current_app.logger.info('Missing data for %d. Backfilling cumulative data from %d' % (collection_date, most_recent_date))
            data = df.loc[df.Date == most_recent_date].copy()
            data['Date'] = collection_date

            # blank out all the outbreak data that should not be copied over, leaving only cumulative data
            cols_to_blank = [col for col in df.columns if col.startswith('Outbrk_')]
            cols_to_blank.extend(['Res_Census', 'Res_Test',	'Staff_Test', 'PPE'])
            data[cols_to_blank] = np.nan

            missing_dates_dfs.append(data)

    processed_df = pd.concat([df, *missing_dates_dfs])
    processed_df.sort_values(
        by=['Facility', 'County', 'City', 'Date'], ignore_index=True, inplace=True)

    return processed_df

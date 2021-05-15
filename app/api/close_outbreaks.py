"""
Closing outbreaks: applies to NM and AR
"""

import flask
import numpy as np
import pandas as pd
from time import time

from app.api import utils


def add_last_reported_now(record, date):
    record['last_recorded'] = date
    return record


def copy_row(new_row, old_row):
    for c in old_row.columns:
        new_row[c] = old_row.iloc[0][c]
    return new_row


def add_info(record, last_collected, all_data, current_date):
    k = str(record['ctp_id'])
    if k in last_collected:
        lr = last_collected[k]

        row = all_data.loc[
            (all_data['Date'] == lr) &
            (all_data['ctp_id'] == record['ctp_id'] )]
        record = copy_row(record, row)

        # clear outbreak columns
        outbreak_cols = utils.make_matching_column_name_map(all_data).values()
        for col in outbreak_cols:
            record[col] = np.nan

        record['last_recorded'] = lr
        record['Date'] = current_date
        record['Outbrk_Status'] = 'Closed'
    else:
        record['last_recorded'] = "Never"

    return record


def close_outbreaks(df):
    filled_in_state = pd.DataFrame()

    df = df[df['Date'].notna()]

    collection_dates = df[['Date']].drop_duplicates()
    collection_dates = collection_dates['Date'].tolist()
    collection_dates.sort()

    # DF subset with these 4 columns
    facilities = df[['ctp_id']].drop_duplicates()

    # store last collection date for each CTP ID
    last_collected = { }

    for collection_date in collection_dates:
        # this is the data block for the current value of collection_date in the loop
        current_block = df.loc[df['Date'] == collection_date]
        for _, block_row in current_block.iterrows():
            k = str(block_row['ctp_id'])
            last_collected[k] = collection_date

        # this selects facilities in the overall set that don't appear for this particular date
        not_in_block = pd.merge(
            current_block, facilities, on = ['ctp_id'],
            how = 'right', indicator = True).loc[lambda x : x['_merge'] == 'right_only']
        not_in_block = not_in_block.apply(
            add_info, axis = 1, args = (last_collected, df, collection_date))
        not_in_block = not_in_block[not_in_block['last_recorded'] != "Never"]

        current_block = current_block.apply(
            add_last_reported_now, axis = 1, args = (collection_date, ) )
        not_in_block = not_in_block.drop(columns = ["_merge"])

        filled_in_state = filled_in_state.append(current_block)
        filled_in_state = filled_in_state.append(not_in_block)

    filled_in_state = filled_in_state.convert_dtypes()
    filled_in_state.reset_index(drop=True, inplace=True)
    return filled_in_state

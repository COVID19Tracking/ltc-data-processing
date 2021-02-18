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
    k = str(record['State']) + str(record['County']) + str(record['City']) + str(record['Facility'])
    if k in last_collected:
        lr = last_collected[k]
        if type(record['County']) is not float and type(record['City']) is not float:
            row = all_data.loc[
                (all_data['Date'] == lr) &
                (all_data['Facility'] == record['Facility']) &
                (all_data['County'] == record['County'])  &
                (all_data['City'] == record['City'] )]
        else:
            row = all_data.loc[
                (all_data['Date'] == lr) &
                (all_data['Facility'] == record['Facility'] )]
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


def clear_outbreak_status(df):
    # for some states like IA, we need to remove outbreak statuses from the final product

    # TODO: put this funcitonality into add_info instead of clearing after the fact
    df['Outbrk_Status'] = ''
    return df


def clear_closed_outbreak_status(df):
    # for states like KS, even as we close an outbreak we can't mark it "closed" so just leaving
    # all "closed" rows blank.

    # TODO: put this funcitonality into add_info instead of clearing after the fact
    df['Outbrk_Status'] = df.Outbrk_Status.apply(lambda x: '' if x == 'Closed' else x)
    return df


def close_outbreaks(df):
    filled_in_state = pd.DataFrame()

    df = df[df['Date'].notna()]

    collection_dates = df[['Date']].drop_duplicates()
    facilities = df[['State', 'County', 'City', 'Facility']].drop_duplicates()

    collection_dates = collection_dates['Date'].tolist()
    collection_dates.sort()

    last_collected = { }

    for collection_date in collection_dates:
        current_block = df.loc[df['Date'] == collection_date]
        for _, block_row in current_block.iterrows():
            k = str(block_row['State']) + str(block_row['County']) + \
                str(block_row['City']) + str(block_row['Facility'])
            last_collected[k] = collection_date

        not_in_block = pd.merge(
            current_block, facilities, on = ['State', 'County', 'City', 'Facility'],
            how = 'right', indicator=True).loc[lambda x : x['_merge']=='right_only']
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

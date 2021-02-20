"""
Ensures that a state has data for every state
Uses the previous week's data if a week is missing.
"""

import flask
import pandas as pd
from time import time

from app.api import utils

ALL_DATES = [
    20200521, 20200528, 20200604, 20200611, 20200618,
    20200625, 20200702, 20200709, 20200716, 20200723,
    20200730, 20200806, 20200813, 20200820, 20200827,
    20200903, 20200910, 20200917, 20200924, 20201001,
    20201008, 20201015, 20201022, 20201029, 20201105,
    20201112, 20201119, 20201126, 20201203, 20201210,
    20201217, 20201224, 20201231, 20210107, 20210114,
    20210121, 20210128, 20210204, 20210211, 20210211
]

def fill_in_missing_dates(df, state_name, onlyThisWeek):
    if onlyThisWeek:
        flask.current_app.logger.info("filling in LATEST missing dates for %s ..." % state_name)
        reversed_all_dates = ALL_DATES[::-1]

        latest_day = reversed_all_dates[0]

        if df['Date'].max() == latest_day:
            return df

        for date in reversed_all_dates:
            if date in df['Date'].tolist():
                flask.current_app.logger.info("filling in %d for %s with data from %d ..." % (latest_day, state_name, date) )
                new_block = df.loc[df['Date'] == date]
                new_block = new_block.assign(Date=latest_day)

                df = df.append(new_block)
                return df

    starting_date = df['Date'].min()

    converted_df = pd.DataFrame()

    skip = {"ND", "OR"}
    if state_name in skip:
        return converted_df

    if starting_date < ALL_DATES[0]:
        starting_date = ALL_DATES[0]

    i = ALL_DATES.index(starting_date)
    prev_date = starting_date

    df = df.convert_dtypes()
    # df = df.astype({'CMS_ID': 'Int64'})

    before = df.loc[df['Date'] < starting_date]
    converted_df = converted_df.append(before)

    while i < len(ALL_DATES):
        date = ALL_DATES[i]

        if date in df['Date'].values:
            converted_df = converted_df.append(df.loc[df['Date'] == date])
            prev_date = date

        else:
            flask.current_app.logger.info("filling in %d for %s with data from %d ..." % (date, state_name, prev_date) )

            new_block = df.loc[df['Date'] == prev_date]
            new_block = new_block.assign(Date=date)

            converted_df = converted_df.append(new_block)

        i += 1

    return converted_df

def do_fill_in_missing_dates(df, onlyThisWeek):
    flask.current_app.logger.info('DataFrame loaded: %d rows' % df.shape[0])

    state = df['State'].iloc[0]
    utils.standardize_data(df)

    t1 = time()
    processed_df = fill_in_missing_dates(df, state, onlyThisWeek)
    t2 = time()

    # this will go into the lambda logs
    flask.current_app.logger.info('Filling in dates for %s took %.1f seconds, %d to %d rows' % (
        state, (t2 - t1), df.shape[0], processed_df.shape[0]))

    return processed_df

def cli_fill_in_missing_dates(outputDir, onlyThisWeek):
    finals = utils.get_all_state_finals_abbrevs()
    utils.run_function_on_states(do_fill_in_missing_dates, [], finals, outputDir, (onlyThisWeek, ) )

"""
Basic data quality checks for facilities sheets.
"""

import flask
import numpy as np
import pandas as pd

from app.api import utils


def check_data_types(df):
    states = set(df['State'])
    if len(states) > 2:
        flask.current_app.logger.error(
            'Some problematic states in State column: %s' % str(states))
    state = [x for x in states if not pd.isnull(x)][0]
    flask.current_app.logger.info('Checking data types for %s...' % state)

    # "Date" column should be dates that look like ints, e.g. 20210128, and they should all be 8
    # chars long when converted to a string
    bad_date_values = set()
    for date in df['Date']:
        if pd.isnull(date):
            bad_date_values.add(np.nan)  # alert to presence of nans, possibly empty rows?
            continue
        try:
            date_int = int(date)
            if len(str(date_int)) != 8:
                raise ValueError
        except Exception as err:
            bad_date_values.add(date)
    if bad_date_values:
        flask.current_app.logger.error(
            'Some problematic data in Date column: %s' % bad_date_values)

    # outbreak open/closed columns should be dates
    for date_col in ['Outbrk_Open', 'Outbrk_Closed']:
        notnull = df[date_col].notnull()
        # where to_datetime fails
        not_datetime = pd.to_datetime(df[date_col], errors='coerce').isna()
        not_datetime = not_datetime & notnull
        bad_date_values = set(df[not_datetime][date_col])
        if bad_date_values:
            flask.current_app.logger.error(
                'Some non-date data in %s column: %s' % (date_col, bad_date_values))

    num_numeric_cols = 24  # total number of metrics
    first_metric_col = 14  # position of 1st metric, "Cumulative Resident Positives"
    for i in range(first_metric_col, first_metric_col + num_numeric_cols):
        # fill missing values with 0s so that it doesn't complain because of nans
        df.iloc[:,i].fillna(0, inplace=True)
        non_int_values = set()
        for val in df.iloc[:,i]:
            try:
                int(val)
            except Exception as err2:
                non_int_values.add(val)
        if non_int_values:
            flask.current_app.logger.error(
                'Some non-int data in %s column: %s' % (df.columns[i], non_int_values))

    # outbreak status should either be "open" or "closed"
    outbreak_statuses = set(df['Outbrk_Status'])
    for status in outbreak_statuses:
        if pd.isnull(status):
            continue
        if status.upper() not in {'OPEN', 'CLOSED'}:
            flask.current_app.logger.error(
                'Unexpected outbreak status: "%s"' % status)

    flask.current_app.logger.info('Done.')


def find_duplicates(df_group, col_map, errors):
    new_df_subset = df_group.loc[df_group['Outbrk_Status'] == 'OPEN'].copy()

    # expecting only one row/open outbreak; if this isn't true, check whether the columns are
    # the same and add the duplicate rows to the errors array
    if new_df_subset.shape[0] > 1:
        deduped = new_df_subset.drop_duplicates()
        if deduped.shape[0] > 1:
            deduped.loc[:, 'error'] = "Multiple open outbreak rows with different data"
        else:
            deduped.loc[:, 'error'] = "Duplicate open outbreak rows with same data"
        errors.append(deduped)


def find_date_duplicates(df_group, col_map, errors):
    # to constrain our checks for data duplicates to the columns with covid data
    data_cols = [*col_map.keys()] + [*col_map.values()]
    dupes = df_group[df_group.duplicated(subset=data_cols, keep=False)]
    dupes['error'] = 'Possible error: rows found with duplicate covid data'
    errors.append(dupes)


def do_quality_checks(df):
    errors = []
    utils.standardize_data(df)
    col_map = utils.make_matching_column_name_map(df)

    # check for duplicate open outbreaks
    df.groupby(
        ['Date', 'Facility', 'County', 'City', 'State_Facility_Type'], as_index=False).apply(
        lambda x: find_duplicates(x, col_map, errors))

    # check for duplicate data on the same date
    df.groupby(
        ['Date'], as_index=False).apply(
        lambda x: find_date_duplicates(x, col_map, errors))

    # get all the errors we found, turn them into a single dataframe
    if errors:
        processed_errors = pd.concat(errors, ignore_index=True)
        return processed_errors
    else:  # no errors!
        return pd.DataFrame()

"""
Basic data quality checks for facilities sheets.
"""

import flask
import pandas as pd

from app.api import utils


def check_data_types(df):
    flask.current_app.logger.info('Checking data types...')

    # "Date" column should be dates that look like ints, e.g. 20210128
    try:
        df['Date'].astype(int)
    except Exception as err:
        if 'Cannot convert non-finite values (NA or inf) to integer' in str(err):
            flask.current_app.logger.error('Some empty values in the Date column')
        else:
            flask.current_app.logger.error(
                'Some problematic non-int data in Date, see error: %s' % err)

    # outbreak open/closed columns should be dates
    for date_col in ['Outbrk_Open', 'Outbrk_Closed']:
        try:
            df[date_col].astype('datetime64[ns]')
        except Exception as err:
            flask.current_app.logger.error(
                'Some non-date data in %s, see error: %s' % (date_col, str(err)))

    num_numeric_cols = 24  # total number of metrics
    first_metric_col = 14  # position of 1st metric, "Cumulative Resident Positives"
    for i in range(first_metric_col, first_metric_col + num_numeric_cols):
        # fill missing values with 0s so that it doesn't complain because of nans
        df.iloc[:,i].fillna(0, inplace=True)
        try:
            df.iloc[:,i].astype(int)
        except Exception as err:
            flask.current_app.logger.error(
                'Some non-int data in %s, see error: %s' % (df.columns[i], str(err)))

    # outbreak status should either be "open" or "closed"
    outbreak_statuses = set(df['Outbrk_Status'])
    for status in outbreak_statuses:
        if pd.isnull(status):
            continue
        if status.upper() not in {'OPEN', 'CLOSED'}:
            flask.current_app.logger.error(
                'Unexpected outbreak status: "%s"' % status)

    flask.current_app.logger.info('Done.')


def cli_quality_checks(outfile, url):
    errors = []

    def find_duplicates(df_group, col_map):
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

    def do_quality_checks(df):
        utils.standardize_data(df)
        col_map = utils.make_matching_column_name_map(df)

        # check for duplicate open outbreaks
        df.groupby(
            ['Date', 'Facility', 'County', 'State_Facility_Type'], as_index=False).apply(
            lambda x: find_duplicates(x, col_map))

        # get all the errors we found, turn them into a single dataframe
        if errors:
            processed_errors = pd.concat(errors, ignore_index=True)
            return processed_errors
        else:  # no errors!
            return pd.DataFrame()

    utils.cli_for_function(do_quality_checks, outfile, url)


def cli_check_data_types(url):
    url = utils.csv_url_for_sheets_url(url)
    df = pd.read_csv(url)
    check_data_types(df)

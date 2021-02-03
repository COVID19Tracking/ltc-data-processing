"""Registers the necessary routes for the API endpoints.

This is a set of LTC data processing scripts, e.g.: aggregates cumulative and outbreak data for
a set of states (e.g. ME) that otherwise have multiple rows per date/facility, in a case of
several outbreaks.
"""

import io
import urllib
from time import time

import flask
from flask import Response
import pandas as pd

from app.api import api, utils


# Uppercases county/city/facility/outbreak status entries, for easier comparison. Modifies in place
def standardize_data(df):
    df[['County', 'City', 'Facility']] = df[['County', 'City', 'Facility']].fillna(value='')
    for colname in ['County', 'City', 'Facility', 'Outbrk_Status']:
        df[colname] = df[colname].str.upper()

    # drop any rows with empty dates
    df.drop(df[pd.isnull(df['Date'])].index, inplace = True)
    df['Date'] = df['Date'].astype(int)


def check_data_types(df):
    flask.current_app.logger.info('Checking data types...')

    # "Date" column should be dates that look like ints, e.g. 20210128
    try:
        df['Date'].astype(int)
    except Exception as err:
        if 'Cannot convert non-finite values (NA or inf) to integer' in str(err):
            flask.current_app.logger.error('Some empty values in the Date column')
        else:
            flask.current_app.logger.error('Some problematic non-int data in Date, see error: %s' % err)

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


def make_matching_column_name_map(df):
    # make a map of corresponding column names (cumulative to current)
    num_numeric_cols = 12  # number of metrics
    first_metric_col = 14  # position of 1st metric, "Cumulative Resident Positives"
    col_map = {}
    for i in range(num_numeric_cols):
        cumulative_col = df.columns[first_metric_col+i]
        outbreak_col = df.columns[first_metric_col+i+num_numeric_cols]
        col_map[cumulative_col] = outbreak_col
    return col_map


# takes a dataframe containing the same facility name/date data and collapses the rows.
# Finds conceptually paired columns based on the content of col_map.
def collapse_rows_new_header_names(df_group, col_map):
    if df_group.shape[0] == 1:
        return df_group

    new_df_subset = df_group.loc[df_group['Outbrk_Status'] == 'OPEN'].copy()
    row_descriptor = '%s %s %s %s' % (
        set(new_df_subset['Facility']),
        set(new_df_subset['State_Facility_Type']),
        set(new_df_subset['County']), 
        set(new_df_subset['Date']))

    # expecting only one row/open outbreak; if this isn't true, check that the columns are the same
    if new_df_subset.shape[0] > 1:
        deduped = new_df_subset.drop_duplicates()
        if deduped.shape[0] > 1:
            flask.current_app.logger.info(
                'Multiple open outbreak rows with different data: %s' % row_descriptor)
        else:
            flask.current_app.logger.info('Duplicate open outbreak rows: %s' % row_descriptor)
            
        return df_group

    if new_df_subset.empty:  # no open outbreaks, but we may want to merge some closed ones
        new_df_subset = df_group.head(1)
        
    for colname in col_map.keys():
        try:
            cumulative_val = df_group[colname].fillna(0).astype(int).sum()
            current_open_val = df_group[col_map[colname]].fillna(0).astype(int).sum()
            val = cumulative_val + current_open_val
            if val > 0:
                index = list(df_group.columns).index(colname)
                new_df_subset.iat[0,index] = val
        except ValueError:  # some date fields in numeric places, return group without collapsing
            flask.current_app.logger.info(
                'Some non-numeric fields in numeric places, column %s: %s' % (
                    colname, row_descriptor))
            return df_group

    return new_df_subset


def preclean_FL(df):
    # these aren't real data rows: dropping 
    df.drop(df[df.County.isin(['TOTAL ICF', 'TOTAL ALF', 'TOTALS'])].index, inplace = True)
    def process_county(county):
        if county in ['UNKNOWN', 'UNKNOWN COUNTY']:
            return ''
        elif county in ['DADE', 'MIAMI-DADE', 'MIAMIͲDADE', 'MIAMI‐DADE']:
            return 'MIAMI-DADE'
        else:
            return county
        
    df['County'] = df['County'].apply(process_county)


def collapse_outbreak_rows(df):
    col_map = make_matching_column_name_map(df)
    # group by facility name and date, collapse each group into one row
    processed_df = df.groupby(
        ['Date', 'Facility', 'County', 'State_Facility_Type'], as_index=False).apply(
        lambda x: collapse_rows_new_header_names(x, col_map))

    processed_df.sort_values(
        by=['Date', 'County', 'City', 'Facility'], inplace=True, ignore_index=True)
    return processed_df


def add_info(record, last_collected, all_data, current_date):
    k = str(record['State']) + str(record['County']) + str(record['City']) + str(record['Facility'])
    if k in last_collected:
        lr = last_collected[k]
        if type(record['County']) is not float and type(record['City']) is not float:
            row = all_data.loc[ (all_data['Date'] == lr) & (all_data['Facility'] == record['Facility']) & (all_data['County'] == record['County'])  & (all_data['City'] == record['City'] )]
        else:
            row = all_data.loc[ (all_data['Date'] == lr) & (all_data['Facility'] == record['Facility'] )]
        record = copy_row(record, row)

        record['last_recorded'] = lr
        record['Date'] = current_date
        record['Outbrk_Status'] = 'Closed'
    else:
        record['last_recorded'] = "Never"

    return record


def add_last_reported_now(record, date):
    record['last_recorded'] = date
    return record


def copy_row(new_row, old_row):
    for c in old_row.columns:
        new_row[c] = old_row.iloc[0][c]
    return new_row


def close_outbreaks(df):
    filled_in_state = pd.DataFrame()

    df = df[df['Date'].notna()]

    collection_dates = df[['Date']].drop_duplicates()
    facilities = df[['State', 'County', 'City', 'Facility']].drop_duplicates()

    last_collected = { }

    for date_index, date_row in collection_dates.iterrows():
        collection_date = date_row['Date']
        current_block = df.loc[df['Date'] == collection_date]
        for _, block_row in current_block.iterrows():
            k = str(block_row['State']) + str(block_row['County']) + str(block_row['City']) + str(block_row['Facility'])
            last_collected[k] = collection_date

        not_in_block = pd.merge(current_block, facilities, on = ['State', 'County', 'City', 'Facility'], how = 'right', indicator=True).loc[lambda x : x['_merge']=='right_only']
        not_in_block = not_in_block.apply(add_info, axis = 1, args = (last_collected, df, collection_date))
        not_in_block = not_in_block[not_in_block['last_recorded'] != "Never"]

        current_block = current_block.apply(add_last_reported_now, axis = 1, args = (collection_date, ) )
        not_in_block = not_in_block.drop(columns = ["_merge"])

        filled_in_state = filled_in_state.append(current_block)
        filled_in_state = filled_in_state.append(not_in_block)

    filled_in_state = filled_in_state.convert_dtypes()
    filled_in_state.reset_index(drop=True, inplace=True)
    return filled_in_state


####################################################################################################
#######################################   API endpoints    #########################################
####################################################################################################


@api.route('/echo', methods=['POST'])
def echo():
    """Dummy route that returns the input data unaltered"""
    payload = flask.request.data
    return Response(payload, mimetype='text/csv')


# Basic URL to hit as a test
@api.route('/test', methods=['GET'])
def test():
    return "Hello World"


# TODO: right now this aggregates outbreaks for whatever state you ask it to do. If we want to start
# chaining functions, we can make a map config for states that specify a chain of functions to apply
# to Pandas DataFrame objects.

def do_aggregate_outbreaks(df):
    flask.current_app.logger.info('DataFrame loaded: %d rows' % df.shape[0])

    # TODO: if more than FL needs special treatment before aggregating outbreaks, factor this out
    # into something nicer
    state = set(df['State']).pop()
    if state == 'FL':
        preclean_FL(df)

    standardize_data(df)

    t1 = time()
    processed_df = collapse_outbreak_rows(df)
    t2 = time()

    # this will go into the lambda logs
    flask.current_app.logger.info('Collapsing %s data took %.1f seconds, %d to %d rows' % (
        state, (t2 - t1), df.shape[0], processed_df.shape[0]))

    return processed_df


@api.route('/aggregate-outbreaks', methods=['POST'])
def api_aggregate_outbreaks():
    payload = flask.request.data.decode('utf-8')
    df = pd.read_csv(io.StringIO(payload))
    processed_df = do_aggregate_outbreaks(df)

    return Response(processed_df.to_csv(index=False), mimetype='text/csv')


def cli_for_function(function, outfile, url):
    """Wrap function in a basic command-line interface that fetches data from a google sheets url

    Function is any function that takes in a pandas dataframe and returns a transformed dataframe"""
    url = utils.csv_url_for_sheets_url(url)

    # fetch the CSV data
    with urllib.request.urlopen(url) as response:
        data = response.read()
    df = pd.read_csv(io.StringIO(data.decode('utf-8')))

    # process it and print the result to STDOUT
    processed_df = function(df)
    if outfile:
        processed_df.to_csv(outfile, index=False)
    else:  # print to STDOUT
        print(processed_df.to_csv(index=False))


def cli_aggregate_outbreaks(outfile, url):
    cli_for_function(do_aggregate_outbreaks, outfile, url)


def do_close_outbreaks_nm_ar(df):
    flask.current_app.logger.info('DataFrame loaded: %d rows' % df.shape[0])

    state = set(df['State']).pop()
    standardize_data(df)

    t1 = time()
    processed_df = close_outbreaks(df)
    t2 = time()

    # this will go into the lambda logs
    flask.current_app.logger.info('Closing outbreaks for %s took %.1f seconds, %d to %d rows' % (
        state, (t2 - t1), df.shape[0], processed_df.shape[0]))

    return processed_df


@api.route('/close-outbreaks-nm-ar', methods=['POST'])
def api_close_outbreaks_nm_ar():
    payload = flask.request.data.decode('utf-8')
    df = pd.read_csv(io.StringIO(payload))
    processed_df = do_close_outbreaks_nm_ar(df)

    return Response(processed_df.to_csv(index=False), mimetype='text/csv')


def cli_close_outbreaks_nm_ar(outfile, url):
    cli_for_function(do_close_outbreaks_nm_ar, outfile, url)


def cli_quality_checks(outfile, url):
    errors = []

    def find_duplicates(df_group, col_map):
        new_df_subset = df_group.loc[df_group['Outbrk_Status'] == 'OPEN'].copy()

        # expecting only one row/open outbreak; if this isn't true, check whether the columns are the same
        # and add the duplicate rows to the errors array
        if new_df_subset.shape[0] > 1:
            deduped = new_df_subset.drop_duplicates()
            if deduped.shape[0] > 1:
                deduped.loc[:, 'error'] = "Multiple open outbreak rows with different data"
            else:
                deduped.loc[:, 'error'] = "Duplicate open outbreak rows with same data"
            errors.append(deduped)

    def do_quality_checks(df):
        # hack: use the latest data
        df = df.loc[df.Date == 20210128]

        standardize_data(df)
        col_map = make_matching_column_name_map(df)

        # check for duplicate open outbreaks
        df.groupby(
            ['Date', 'Facility', 'County', 'State_Facility_Type'], as_index=False).apply(
            lambda x: find_duplicates(x, col_map))

        # get all the errors we found, turn them into a single dataframe
        processed_errors = pd.concat(errors, ignore_index=True)
        return processed_errors

    cli_for_function(do_quality_checks, outfile, url)


def cli_check_data_types(url):
    url = utils.csv_url_for_sheets_url(url)

    # fetch the CSV data
    with urllib.request.urlopen(url) as response:
        data = response.read()
    df = pd.read_csv(io.StringIO(data.decode('utf-8')))

    # hack: use the latest data
    df = df.loc[df.Date == 20210128]
    check_data_types(df)

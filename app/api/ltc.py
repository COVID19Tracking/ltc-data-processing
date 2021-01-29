"""Registers the necessary routes for the API endpoints.

This is a set of LTC data processing scripts, e.g.: aggregates cumulative and outbreak data for
a set of states (e.g. ME) that otherwise have multiple rows per date/facility, in a case of
several outbreaks.
"""

import io
import json
from time import time

import flask
from flask import Response
import pandas as pd

from app.api import api


# Uppercases county/city/facility/outbreak status entries, for easier comparison. Modifies in place
def standardize_data(df):
    df[['County', 'City', 'Facility']] = df[['County', 'City', 'Facility']].fillna(value='')
    for colname in ['County', 'City', 'Facility', 'Outbrk_Status']:
        df[colname] = df[colname].str.upper()

    # drop any rows with empty dates
    df.drop(df[pd.isnull(df['Date'])].index, inplace = True)
    df['Date'] = df['Date'].astype(int)


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

@api.route('/aggregate-outbreaks', methods=['POST'])
def aggregate_outbreaks():
    payload = flask.request.data.decode('utf-8')
    df = pd.read_csv(io.StringIO(payload))
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

    return Response(processed_df.to_csv(index=False), mimetype='text/csv')

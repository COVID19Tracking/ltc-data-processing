from os import path

from app.api import fill_missing_dates
from app.api.gsheets import csv_url_for_sheets_url, save_to_sheet
import pandas as pd


def get_all_state_urls():
    # TODO: move this out into something like config.py so it's not buried here
    url_link = 'https://docs.google.com/spreadsheets/d/1kBL149bp8PWd_NMFm8Gxj-jXToSNEU9YNgQs0o9tREs/gviz/tq?tqx=out:csv&sheet=State_links'
    url_df = pd.read_csv(url_link)
    return url_df

def get_entry_url(state, url_df):
    return url_df.loc[url_df.State == state].iloc[0].Entry


def get_final_url(state, url_df):
    return url_df.loc[url_df.State == state].iloc[0].Final


def get_second_final_url(state, url_df):
    return url_df.loc[url_df.State == state].iloc[0].Final2


# Using the standard facility sheet organization, creates a column name map for corresponding column
# names, cumulative -> current outbreak metric columns.
def make_matching_column_name_map(df):
    num_numeric_cols = 12  # number of metrics
    first_metric_col = 14  # position of 1st metric, "Cumulative Resident Positives"
    col_map = {}
    for i in range(num_numeric_cols):
        cumulative_col = df.columns[first_metric_col+i]
        outbreak_col = df.columns[first_metric_col+i+num_numeric_cols]
        col_map[cumulative_col] = outbreak_col
    return col_map


# Uppercases county/city/facility/outbreak status entries, for easier comparison. Modifies in place
def standardize_data(df):
    df[['County', 'City', 'Facility', 'Outbrk_Status', 'State_Facility_Type', 'CTP_Facility_Type']] = \
        df[['County', 'City', 'Facility', 'Outbrk_Status', 'State_Facility_Type', 'CTP_Facility_Type']].fillna(value='')
    for colname in ['County', 'City', 'Facility', 'Outbrk_Status', 'State_Facility_Type', 'CTP_Facility_Type']:
        df[colname] = df[colname].str.upper().str.strip()

    # drop any rows with empty dates
    df.drop(df[pd.isnull(df['Date'])].index, inplace = True)
    df['Date'] = df['Date'].astype(int)

    # remove newlines from facility names
    df['Facility'] = df['Facility'].str.replace('\n', ' ')

    # remove unwanted special character from facility name
    df['Facility'] = df['Facility'].str.replace(' §', '')

    # drop full duplicates
    df.drop_duplicates(inplace=True)

    return df


# fill in missing dates and sort output. Modifies in place
# if close_unknown_outbreaks is true, weeks with missing outbreak status will be closed
def post_processing(df, close_unknown_outbreaks=False):
    df = fill_missing_dates.fill_missing_dates(df)

    if close_unknown_outbreaks:
        df['Outbrk_Status'].fillna('Closed', inplace=True)

    df.sort_values(
        by=['Facility', 'County', 'City', 'State_Facility_Type', 'Date'], ignore_index=True, inplace=True)
    return df


def cli_for_function(function, outfile, url, write_to_sheet=False):
    """Wrap function in a basic command-line interface that fetches data from a google sheets url

    Function is any function that takes in a pandas dataframe and returns a transformed dataframe"""

    # URL can be a local CSV or a link
    if not url.endswith('.csv'):
        url = csv_url_for_sheets_url(url)
    df = pd.read_csv(url)

    # process it and send the result to the appropriate place
    processed_df = function(df)

    if write_to_sheet:
        save_to_sheet(write_to_sheet, processed_df)

    if outfile and not processed_df.empty:
        processed_df.to_csv(outfile, index=False)
    else:  # print to STDOUT
        print(processed_df.to_csv(index=False))

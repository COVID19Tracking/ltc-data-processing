import click
import io
import re

from os import path

import pandas as pd


def csv_url_for_sheets_url(url):
    """extract the parameters from a google docs url and formulate a CSV export url"""
    m = re.search('.*\/d\/(.*)\/edit.*#gid=(.*)', url)
    if m:
        key = m.group(1)
        gid = m.group(2)
        return f"https://docs.google.com/spreadsheets/d/{key}/export?format=csv&gid={gid}"
    else:
        click.echo('Invalid Google Sheets URL')
        raise click.Abort()


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
    df[['County', 'City', 'Facility', 'Outbrk_Status']] = \
        df[['County', 'City', 'Facility', 'Outbrk_Status']].fillna(value='')
    for colname in ['County', 'City', 'Facility', 'Outbrk_Status']:
        df[colname] = df[colname].str.upper()

    # drop any rows with empty dates
    df.drop(df[pd.isnull(df['Date'])].index, inplace = True)
    df['Date'] = df['Date'].astype(int)


def cli_for_function(function, outfile, url):
    """Wrap function in a basic command-line interface that fetches data from a google sheets url

    Function is any function that takes in a pandas dataframe and returns a transformed dataframe"""
    url = csv_url_for_sheets_url(url)
    df = pd.read_csv(url)

    # process it and print the result to STDOUT
    processed_df = function(df)
    if outfile:
        processed_df.to_csv(outfile, index=False)
    else:  # print to STDOUT
        print(processed_df.to_csv(index=False))

def get_all_state_finals():
    states_docs_urls = pd.read_csv("app/api/state_docs_urls.csv")
    return states_docs_urls['Finals'].tolist()

def get_all_priortize_entries():
    entries, finals = [], []
    for _, state_row in states_docs_urls.iterrows():
        if state_row['Entry']:
            entries.append(state_row['Entry'])
        else:
            finals.append(state_row['Final'])

    return (entries, finals)

def run_function_on_states(function, entries, finals, outputDir):
    """
    Call the function on every google sheets url for the specified states.
    """
    states_docs_urls = pd.read_csv("app/api/state_docs_urls.csv")

    def process(state_row, column):
        print("Running function %s on %s %s sheet..." % (function.__name__, state_row['State'], column))
        url = csv_url_for_sheets_url(state_row[column])
        df = pd.read_csv(url)
        processed_df = function(df)
        return processed_df

    for _, state_row in states_docs_urls.iterrows():
        if state_row['Entry'] and state_row['State'] in entries:
            processed_df = process(state_row, 'Entry')
            if not processed_df.empty:
                processed_df.to_csv(path.join(outputDir, "%s_processed_entry.csv" % state_row['State']), index=False)

        elif state_row['Final'] and state_row['State'] in finals:
            processed_df = process(state_row, 'Final')
            if not processed_df.empty:
                processed_df.to_csv(path.join(outputDir, "%s_processed_final.csv" % state_row['State']), index=False)

        else:
            print("Skipping %s..." % state_row['State'])

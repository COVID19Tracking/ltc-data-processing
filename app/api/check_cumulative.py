"""
Check for cumulative data decreases from week to week.
"""
import csv
from time import time

import flask
import pandas as pd

from app.api import utils

def do_check_cumulative(outputFile, onlyThisWeek):
    states = utils.get_all_state_finals()

    with open(outputFile, mode='w') as csv_file:
        fieldnames = [
                'State',
                'Facility',
                'cumulative_field_decresed',
                'Date',
                'current_cumulative_value',
                'Prev Date',
                'prev_cumulative_value',
                'CTP_Facility_Type'
            ]

        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        def check_cumulative(df):
            collection_dates = df[['Date']].drop_duplicates()
            this_week = collection_dates.max()

            facilities = df[['Facility', 'County', 'City', 'CTP_Facility_Type']].drop_duplicates()
            cume_cols = [x for x in df.columns if x.startswith('Cume_')]

            for _, row in facilities.iterrows():
                facility = df.loc[
                    (df.Facility == row.Facility) &
                    (df.County == row.County) &
                    (df.City == row.City) &
                    (df.CTP_Facility_Type == row.CTP_Facility_Type)].reset_index()

                facility = facility.sort_values(by=['Date'])

                for f_index, f_row in facility.iterrows():
                    if f_index == 0:
                        continue

                    for col in cume_cols:
                        row_to_write = None
                        if f_row[col] != -1 and f_row[col] < int(facility.loc[f_index-1, col]):
                            row_to_write = {'State': f_row['State'],
                                'Facility': f_row['Facility'],
                                'Date': f_row['Date'],
                                'cumulative_field_decresed': col,
                                'current_cumulative_value': int(f_row[col]),
                                'Prev Date': facility.loc[f_index-1, 'Date'],
                                'prev_cumulative_value': int(facility.loc[f_index-1, col]),
                                'CTP_Facility_Type': f_row['CTP_Facility_Type']}

                        if row_to_write:
                            if not onlyThisWeek:
                                writer.writerow(row_to_write)

                            elif onlyThisWeek and collection_date == this_week:
                                writer.writerow(row_to_write)

                            break

        t1 = time()

        for state in states:

            url = utils.csv_url_for_sheets_url(state)
            df = pd.read_csv(url)

            state_name = set(df['State']).pop()
            flask.current_app.logger.info('Generating quality report for %s' % state_name)

            utils.standardize_data(df)
            extra_data_standardization(df, state_name)

            check_cumulative(df)

        t2 = time()

        # this will go into the lambda logs
        flask.current_app.logger.info('Generating quality report took %.1f seconds' % (t2 - t1))

def transform_RI(record):
    if type(record) is str:
        return int(record.split(' ')[0])

def extra_data_standardization(df, state_name):
    cume_cols = [x for x in df.columns if x.startswith('Cume_')]

    if state_name == "RI":
        df['Cume_Res_Pos'] = df['Cume_Res_Pos'].apply(transform_RI)
        df['Cume_Res_Death'] = -1

    df[cume_cols] = df[cume_cols].fillna(value=-1)

    if state_name == "MA":
        ## These are very broad and don't tell us much
        df["Cume_ResStaff_Pos"] = -1

    df[cume_cols] = df[cume_cols].replace("<11", 1)
    df[cume_cols] = df[cume_cols].replace("1-4", 1)
    df[cume_cols] = df[cume_cols].replace("< 5", 1)
    df[cume_cols] = df[cume_cols].replace("43^", 44)
    df[cume_cols] = df[cume_cols].replace("58 *", 58)
    df[cume_cols] = df[cume_cols].replace("0 *", 0)
    df[cume_cols] = df[cume_cols].replace("8 *", 8)
    df[cume_cols] = df[cume_cols].replace("60 *", 60)
    df[cume_cols] = df[cume_cols].replace("4 *", 4)
    df[cume_cols] = df[cume_cols].replace("55 *", 55)
    df[cume_cols] = df[cume_cols].replace("56 *", 56)
    df[cume_cols] = df[cume_cols].replace("18 *", 18)
    df[cume_cols] = df[cume_cols].replace("25 *", 25)
    df[cume_cols] = df[cume_cols].replace("3 *", 3)
    df[cume_cols] = df[cume_cols].replace("2 *", 2)
    df[cume_cols] = df[cume_cols].replace("4*", 4)
    df[cume_cols] = df[cume_cols].replace("0 ^", 0)
    df[cume_cols] = df[cume_cols].replace("35 ^", 35)
    df[cume_cols] = df[cume_cols].replace("6 *", 6)
    df[cume_cols] = df[cume_cols].replace("≤5", 1)
    df[cume_cols] = df[cume_cols].replace("Redacted (1-4 cases)", 1) # PA
    df[cume_cols] = df[cume_cols].replace("Redacted (1-4 deaths)", 1) # PA
    df[cume_cols] = df[cume_cols].replace(" ", -1) # PA

    strings_to_drop = {'#VALUE!', 'Data Pending', 'Not Reported', 'Closed',
            '02/02/1900', '--', '-', '.', '*', 'No Data', 'No data', 'No data ',
            '[;', 'ND', 'NO DATA', 'NO DATA ', 'COVID-19 Positive Residents'}

    df.drop(df[df[cume_cols].isin(strings_to_drop).any(1)].index, inplace = True)

    df[cume_cols] = df[cume_cols].astype(int)

def cli_check_cumulative_data(outputFile, onlyThisWeek):
    do_check_cumulative(outputFile, onlyThisWeek)

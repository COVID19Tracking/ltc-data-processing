"""
Check for cumulative data decreases from week to week.
"""
import csv

import flask
import pandas as pd

from app.api import utils

def check_cumulative(df, onlyThisWeek=False):
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

    errors = pd.DataFrame(columns=fieldnames)

    state_name = set(df['State']).pop()
    flask.current_app.logger.info('Finding decreased cumulative fields for %s' % state_name)

    extra_data_standardization(df, state_name)

    this_week = df['Date'].max()
    facilities = df[['Facility', 'County', 'City', 'CTP_Facility_Type']].drop_duplicates()
    cume_cols = [x for x in df.columns if x.startswith('Cume_')]

    for _, row in facilities.iterrows():
        facility = df.loc[
            (df.Facility == row.Facility) &
            (df.County == row.County) &
            (df.City == row.City) &
            (df.CTP_Facility_Type == row.CTP_Facility_Type)].reset_index()

        facility = facility.sort_values(by=['Date']).reset_index()

        for f_index, f_row in facility.iterrows():
            if f_index == 0:
                continue

            for col in cume_cols:
                if f_row[col] != -1 and f_row[col] < int(facility.loc[f_index-1, col]):
                    row_to_write = {'State': f_row['State'],
                        'Facility': f_row['Facility'],
                        'Date': f_row['Date'],
                        'cumulative_field_decresed': col,
                        'current_cumulative_value': int(f_row[col]),
                        'Prev Date': facility.loc[f_index-1, 'Date'],
                        'prev_cumulative_value': int(facility.loc[f_index-1, col]),
                        'CTP_Facility_Type': f_row['CTP_Facility_Type']}

                    if not onlyThisWeek or (onlyThisWeek and f_row['Date'] == this_week):
                        print(f_row['Date'])
                        errors = errors.append(row_to_write, ignore_index=True)

                    break
    return errors

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

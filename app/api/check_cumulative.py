"""
Check for cumulative data decreases from week to week.
"""
import csv
from time import time

import flask
import pandas as pd

from app.api import utils

def do_check_cumulative(outputFile):
    states = utils.get_all_state_finals()

    with open(outputFile, mode='w') as csv_file:
        fieldnames = ['State', 'Facility', 'Date', 'cume_res_pos_value', 'Prev Date', 'prev_cume_res_pos_value', 'CTP_Facility_Type']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        def check_cumulative(df):
            collection_dates = df[['Date']].drop_duplicates()

            last_cumes = {}
            last_cume = 0

            collection_dates = collection_dates['Date'].tolist()
            collection_dates.sort()

            for collection_date in collection_dates:
                current_block = df.loc[df['Date'] == collection_date]

                for _, f_row in current_block.iterrows():
                    current_cume = f_row['Cume_Res_Pos']
                    k = str(f_row['State']) + str(f_row['County']) + str(f_row['City']) + str(f_row['Facility'])
                    if k in last_cumes:
                        if int(last_cumes[k][0]) > int(current_cume) and last_cumes[k][1] <= collection_date:
                            writer.writerow({'State': f_row['State'],
                                'Facility': f_row['Facility'],
                                'Date': collection_date,
                                'cume_res_pos_value': int(current_cume),
                                'Prev Date': last_cumes[k][1],
                                'prev_cume_res_pos_value': int(last_cumes[k][0]),
                                'CTP_Facility_Type': f_row['CTP_Facility_Type']})

                    last_cumes[k] = (current_cume, collection_date)

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

def transform_RI(cume_res_pos):
    return int(cume_res_pos.split(' ')[0])

def extra_data_standardization(df, state_name):
    df.drop(df[pd.isnull(df['Cume_Res_Pos'])].index, inplace = True)

    if state_name == "RI":
        df['Cume_Res_Pos'] = df['Cume_Res_Pos'].apply(transform_RI)

    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("<11", 1)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("< 5", 1)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("43^", 44)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("58 *", 58)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("0 *", 0)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("8 *", 8)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("60 *", 60)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("4 *", 4)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("55 *", 55)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("56 *", 56)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("18 *", 18)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("25 *", 25)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("3 *", 3)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("2 *", 2)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("4*", 4)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("0 ^", 0)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("35 ^", 35)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("6 *", 6)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("≤5", 1)
    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].replace("Redacted (1-4 cases)", 1) # PA

    strings_to_drop = {'#VALUE!', 'Data Pending', 'Not Reported', 'Closed',
            '02/02/1900', '--', '-', '.', '*', 'No Data', 'No data', 'No data ',
            '[;', 'ND', 'COVID-19 Positive Residents'}

    df.drop(df[df['Cume_Res_Pos'].isin(strings_to_drop)].index, inplace = True)

    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].astype(int)

def cli_check_cumulative_data(outputFile):
    do_check_cumulative(outputFile)

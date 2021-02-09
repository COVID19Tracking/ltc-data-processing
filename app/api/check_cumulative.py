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
        fieldnames = ['State', 'Facility', 'Date', 'cume_res_pos_value', 'prev_cume_res_pos_value']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        def check_cumulative(df):
            collection_dates = df[['Date']].drop_duplicates()

            last_cumes = {}
            last_cume = 0

            for date_index, date_row in collection_dates.iterrows():
                collection_date = date_row['Date']
                current_block = df.loc[df['Date'] == collection_date]

                for _, f_row in current_block.iterrows():
                    current_cume = f_row['Cume_Res_Pos']
                    k = str(f_row['State']) + str(f_row['County']) + str(f_row['City']) + str(f_row['Facility'])
                    if k in last_cumes:
                        if int(last_cumes[k]) > int(current_cume):
                            writer.writerow({'State': f_row['State'], 'Facility': f_row['Facility'], 'Date': collection_date, 'cume_res_pos_value': int(current_cume), 'prev_cume_res_pos_value': int(last_cumes[k])})

                    last_cumes[k] = current_cume

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

    df.drop(df[df['Cume_Res_Pos'] == "#VALUE!"].index, inplace = True)
    df.drop(df[df['Cume_Res_Pos'] == "Data Pending"].index, inplace = True)
    df.drop(df[df['Cume_Res_Pos'] == "Not Reported"].index, inplace = True)
    df.drop(df[df['Cume_Res_Pos'] == "Closed"].index, inplace = True)
    df.drop(df[df['Cume_Res_Pos'] == "02/02/1900"].index, inplace = True) ##Maine MAINE VETERANS HOME 20200827
    df.drop(df[df['Cume_Res_Pos'] == "--"].index, inplace = True) ## MI
    df.drop(df[df['Cume_Res_Pos'] == "-"].index, inplace = True) ## MI
    df.drop(df[df['Cume_Res_Pos'] == "."].index, inplace = True) ## MI
    df.drop(df[df['Cume_Res_Pos'] == "*"].index, inplace = True) ## PA
    df.drop(df[df['Cume_Res_Pos'] == "No Data"].index, inplace = True) ## PA
    df.drop(df[df['Cume_Res_Pos'] == "No data"].index, inplace = True) ## PA
    df.drop(df[df['Cume_Res_Pos'] == "[;"].index, inplace = True) ## PA
    df.drop(df[df['Cume_Res_Pos'] == "No data "].index, inplace = True) ## PA
    df.drop(df[df['Cume_Res_Pos'] == "ND"].index, inplace = True) ## PA
    df.drop(df[df['Cume_Res_Pos'] == "COVID-19 Positive Residents"].index, inplace = True) ## PA

    df[['Cume_Res_Pos']] = df[['Cume_Res_Pos']].astype(int)

def cli_check_cumulative_data(outputFile):
    do_check_cumulative(outputFile)

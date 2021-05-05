"""
The functions in this file relate to adding cumulative case/death numbers assuming a weird data
reset in KY.
"""

import flask
import numpy as np
import pandas as pd
from time import time

from app.api import utils


def preclean_KY(df):
    # some county names are "Jefferson" vs "Jefferson County", so standardizing by removing "County"
    # and fixing some county typos while here
    replace_strings = {
        ' COUNTY': '',
        'BULLIT': 'BULLITT',
        'SHLEBY': 'SHELBY',
        'WOODFOORD': 'WOODFORD',
    }
    for key, val in replace_strings.items():
        df['County'] = df['County'].str.replace(key, val)

    return df


# are there any facilities that have dates in 2021 that aren't consecutive from the max? If this
# returns False, we just propagate the group as is because there's something weird about the dates
def is_2021_info_okay(df_group, dates_2021):
    df_group_dates_2021 = set(x for x in set(df_group.Date) if x > 20210000)
    seen_2021 = False
    for date in dates_2021:
        if seen_2021 and date not in df_group_dates_2021:
            print('%s' % set(df_group.Facility))
            return False
        if not seen_2021 and date in df_group_dates_2021:
            seen_2021 = True
    return True


# Adds extra rows for 2021 if any are missing, and recomputes the cumulative values using the counts
# from 20201231 as the base: KY reset their cumulative counts in 2021 so we're correcting for that.
#
# If any facilities used to be reported but were dropped before 20201231, add them to the
# early_disappeared_facilities set.
def update_ky_2021_data(df_group, col_map, dates_2021, early_disappeared_facilities):
    # if something is wrong with this group, flag, return
    if not is_2021_info_okay(df_group, dates_2021):
        return df_group
    
    # if we don't have data for this facility on 12/31/2020, flag it as a possible other issue,
    # return group
    if 20201231 not in set(df_group.Date):
        early_disappeared_facilities.update(df_group.Facility)
        return df_group
    
    # create rows for missing 2021 dates with no data
    missing_2021_dates = set(dates_2021).difference(set(df_group.Date))
    if len(missing_2021_dates) > 0:
        # take the 20201231 data and propagate it forward, set the dates, remove all metric counts
        df_head = df_group.head(1)
        replicated_row_df = df_head.loc[df_head.index.repeat(len(missing_2021_dates))]
        replicated_row_df['Date'] = missing_2021_dates
        for col1, col2 in col_map.items():
            replicated_row_df.loc[:, col1] = np.nan
            replicated_row_df.loc[:, col2] = np.nan
    
        df_group = pd.concat([df_group, replicated_row_df])
        df_group.sort_values(
            by=['Date', 'ctp_id'], inplace=True, ignore_index=True)

    # within each group, take the case/death numbers as of 20201231. Use that as the base number for
    # when they reset in 2021. Set each 2021 cumulative value to the sum of what's there now and
    # what was on 12/31
    base_row = df_group.loc[df_group.Date == 20201231].iloc[0]
    for i, row in df_group.iterrows():
        if row.Date < 20210000:  # skip 2020 rows
            continue

        for col in ['Cume_Res_Pos', 'Cume_Staff_Pos', 'Cume_Res_Death', 'Cume_Staff_Death']:
            base_val = base_row[col]
            cur_val = df_group.loc[i, col]
            if pd.isnull(cur_val):
                cur_val = 0
            df_group.at[i, col] = base_val + cur_val

    return df_group


def really_update_ky_2021_data(df):
    col_map = utils.make_matching_column_name_map(df)
    dates_2021 = sorted([x for x in set(df.Date) if x > 20210101])

    # group facilities and update each group as needed
    early_disappeared_facilities = set()
    processed_df = df.groupby(['ctp_id'], as_index=False).apply(
        lambda x: update_ky_2021_data(x, col_map, dates_2021, early_disappeared_facilities))

    # log which facilities disappeared before 20201231
    if len(early_disappeared_facilities) > 0:
        flask.current_app.logger.info('Facilities dropped before 20201231:')
        flask.current_app.logger.info(early_disappeared_facilities)

    return processed_df

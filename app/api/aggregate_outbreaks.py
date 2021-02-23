"""
The functions in this file relate to aggregating cumulative and outbreak data for
a set of states (e.g. ME) that otherwise have multiple rows per date/facility, in a case of
several outbreaks.
"""

import flask
import numpy as np
import pandas as pd
from time import time

from app.api import utils


####################################################################################################
#######################################   FL-specific functions    #################################
####################################################################################################


def preclean_FL(df):
    # these aren't real data rows: dropping 
    df.drop(df[df.County.isin(['TOTAL ICF', 'TOTAL ALF', 'TOTALS'])].index, inplace = True)

    # some facilities have weird characters, replace as needed
    df['Facility'] = df['Facility'].str.replace('Ͳ','-')
    df['County'] = df['County'].str.replace('Ͳ','-')
    df['Facility'] = df['Facility'].str.replace('‐', '-')  # insane ascii stuff
    def process_county(county):
        if county in ['UNKNOWN', 'UNKNOWN COUNTY']:
            return ''
        elif county in ['DADE', 'MIAMI-DADE', 'MIAMIͲDADE', 'MIAMI‐DADE']:
            return 'MIAMI-DADE'
        else:
            return county
        
    df['County'] = df['County'].apply(process_county)
    return df


# drop rows that have no data
# this is optimized for FL
def drop_null_row_FL(df):
    df = df.dropna(how='all', subset=['Outbrk_Status', 'Res_Census', 'Cume_Res_Death', 'Cume_Staff_Death', 'Cume_ResStaff_Death', 'Cume_ResStaff_ ProbDeath', 'Outbrk_Res_Pos', 'Outbrk_Staff_Pos'])
    return df


# fills empty State Facility Types and CMS IDs using matching facility rows with types
# this is optimized for FL
def fill_state_facility_type_FL(df):

    # process records that have no state type
    def process_no_type(record):
        # if there is alreay a state type, return
        if(not record['State_Facility_Type'] == ''):
            return record

        # grab all facilities with matching names
        matches = df.loc[(df['Facility'] == record['Facility']) & ~(df['State_Facility_Type'] == '')]
        matches = matches.dropna(subset=['State_Facility_Type'])
        facility_types = matches['State_Facility_Type'].unique()

        # if both NH and ALF/ICF appear in the types for matching facilities, return
        # this is to be extra conservative
        if(('ALF' in facility_types or 'ICF' in facility_types) and 'NH' in facility_types):
            return record

        # grab most common type and CMS id amongst the matches
        # this is probably redundant but could catch issues like a non-repeated typo in a CMS id
        ftype = matches['State_Facility_Type'].mode()
        matches = matches.dropna(subset=['CMS_ID'])
        cms = matches['CMS_ID'].mode()

        # if there is no matching state type, return
        if(ftype.empty):
            return record
        else:
            record['State_Facility_Type'] = ftype[0]

        # if there is no matching CMS id, return (e.g. the ftype is ALF and there is no CMS ID found)
        if(cms.empty):
            return record
        else:
            # only set new CMS ID if the ftype is NH
            if(not ftype[0] == 'NH'):
                return record
            record['CMS_ID'] = cms[0]
        return record
    df = df.apply(process_no_type, axis = 1)
    return df


# fills rows with an empty county using matching facility rows
# this is optimized for FL
def fill_county_FL(df):

    # process records that have no county
    def process_no_type(record):
        # if there is alreay a county, return
        if(not record['County'] == ''):
            return record

        # grab all facilities with matching names that have assigned counties
        matches = df.loc[(df['Facility'] == record['Facility']) & ~(df['County'] == '')]

        ftype = record['State_Facility_Type']
        # if the record has a specified type, only use matches with the same type
        if(ftype):
            matches = matches.loc[(matches['State_Facility_Type'] == ftype)]

        # grab unique results
        county = matches['County'].unique()

        # if there is no match or more than one, return
        if(county.shape[0] > 1 or county.shape[0] == 0):
            return record
        else:
            record['County'] = county[0]
        return record
    df = df.apply(process_no_type, axis = 1)
    return df


# sets outbreak status to OPEN if facility has outbreak cases, and removes OPEN if no cases are listed
# this is optimized for FL
def fill_outbreak_status_FL(df):

    def set_outbreak(record):
        if(not pd.isnull(record['Outbrk_Res_Pos']) or not pd.isnull(record['Outbrk_Staff_Pos'])):
            record['Outbrk_Status'] = 'OPEN'
        else:
            record['Outbrk_Status'] == ''
        return record
    df = df.apply(set_outbreak, axis = 1)
    return df


# cleans up CTP Facility Types and federal/state regulated
# this is optimized for FL - other states have different labels
def state_to_ctp_FL(record):
    state = record['State_Facility_Type']
    if(state == 'ALF' or state == 'Assisted Living'):
        record['CTP_Facility_Type'] = 'Assisted Living'
        record['Regulate'] = 'State'
    elif(state == 'NH'):
        record['CTP_Facility_Type'] = 'Nursing Home'
        record['Regulate'] = 'Federal'
    elif(state == 'ICF'):
        record['CTP_Facility_Type'] = 'Other'
        record['Regulate'] = 'State'
    else:
        record['CTP_Facility_Type'] = np.nan
        record['Regulate'] = np.nan
    return record


# clears any CMS IDs tied to facilities that are not nursing homes
# this is optimized for FL - other states have different labels
def clear_non_nh_cms_ids_FL(record):
    if ((record['State_Facility_Type'] != 'NH') and (not pd.isnull(record['CMS_ID']))):
        record['CMS_ID'] = np.nan
    return record


def postclean_FL(df):
    df = df.apply(state_to_ctp_FL, axis = 1)
    df = df.apply(clear_non_nh_cms_ids_FL, axis = 1)
    return df


####################################################################################################
#######################################   Aggregation logic    #####################################
####################################################################################################


# takes a dataframe containing the same facility name/date data and collapses the rows.
# Finds conceptually paired columns based on the content of col_map.
def collapse_rows_new_header_names(df_group, col_map, add_outbreak_and_cume=True):
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
            return df_group
        else:
            # still duplicate rows, but these are the same data so we can trust it
            flask.current_app.logger.info('Duplicate open outbreak rows: %s' % row_descriptor)
            new_df_subset = deduped

    if new_df_subset.empty:  # no open outbreaks, but we may want to merge some closed ones
        new_df_subset = df_group.head(1)
        
    for colname in col_map.keys():
        try:
            cumulative_val = df_group[colname].fillna(0).astype(int).sum()
            current_open_val = int(new_df_subset[col_map[colname]].fillna(0))

            if add_outbreak_and_cume:
                val = cumulative_val + current_open_val
            else:
                val = cumulative_val

            if val > 0:
                index = list(df_group.columns).index(colname)
                new_df_subset.iat[0,index] = val
        except ValueError:  # some date fields in numeric places, return group without collapsing
            flask.current_app.logger.info(
                'Some non-numeric fields in numeric places, column %s: %s' % (
                    colname, row_descriptor))
            return df_group

    return new_df_subset


def collapse_outbreak_rows(df, add_outbreak_and_cume=True):
    col_map = utils.make_matching_column_name_map(df)
    # group by facility name and date, collapse each group into one row
    processed_df = df.groupby(
        ['Date', 'Facility', 'County', 'State_Facility_Type'], as_index=False).apply(
        lambda x: collapse_rows_new_header_names(
            x, col_map, add_outbreak_and_cume=add_outbreak_and_cume))

    processed_df.sort_values(
        by=['Date', 'County', 'City', 'Facility'], inplace=True, ignore_index=True)
    return processed_df


# applies to CA: takes groups with 1 each of open and closed data points, combines into one "open"
# row moving the cumulative data into that row - does not add anything, propagates whatever is there
def combine_open_closed_info_do_not_add(df_group, col_map, restrict_facility_types=False):
    if df_group.shape[0] != 2:  # only dealing with cases where there are 2 rows we need to collapse
        return df_group
    new_df_subset = df_group.head(1)
    row_descriptor = '%s %s %s %s' % (
        set(new_df_subset['Facility']),
        set(new_df_subset['State_Facility_Type']),
        set(new_df_subset['County']),
        set(new_df_subset['Date']))
    facility_type = set(df_group.State_Facility_Type).pop()
    if restrict_facility_types and facility_type not in ['RESIDENTIAL CARE', 'RCFE']:
        return df_group
    
    # start with the "open" outbreak row, copy over cumulative data from the other row
    open_row_df = df_group.loc[df_group['Outbrk_Status'] == 'OPEN'].copy()
    if(open_row_df.shape[0] < 1):
        flask.current_app.logger.info(
                'Multiple non-open rows with different data: %s' % row_descriptor)
        return df_group
    elif(open_row_df.shape[0] > 1):
        flask.current_app.logger.info(
                'Multiple open rows with different data: %s' % row_descriptor)
        return df_group
    closed_row = df_group.loc[df_group['Outbrk_Status'] != 'OPEN'].iloc[0]
    for cume_col in col_map.keys():
        open_row_df[cume_col] = closed_row[cume_col]
    
    return open_row_df


def collapse_facility_rows_no_adding(df, restrict_facility_types=False):
    col_map = utils.make_matching_column_name_map(df)
    processed_df = df.groupby(
        ['Date', 'Facility', 'County', 'State_Facility_Type'], as_index=False).apply(
        lambda x: combine_open_closed_info_do_not_add(
            x, col_map, restrict_facility_types=restrict_facility_types))
    processed_df.sort_values(
        by=['Date', 'County', 'City', 'Facility'], inplace=True, ignore_index=True)
    return processed_df

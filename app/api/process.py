"""
The main "processing" module.
"""

import os

import flask
import numpy as np
import pandas as pd
from time import time

from app.api import utils, aggregate_outbreaks, close_outbreaks, data_quality_checks, \
    unreset_cumulative, replace_no_data, check_cumulative, drop_no_data


_FUNCTION_LISTS = {
    'AR': [utils.standardize_data, close_outbreaks.close_outbreaks, utils.post_processing],
    'CA': [
        utils.standardize_data,
        lambda df: aggregate_outbreaks.collapse_facility_rows_no_adding(
            df, restrict_facility_types=True),
        utils.post_processing
        ],
    'CO': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows, utils.post_processing],
    'CT': [utils.standardize_data, utils.post_processing],
    'DC': [utils.standardize_data, utils.post_processing],
    'DE': [utils.standardize_data, aggregate_outbreaks.collapse_facility_rows_no_adding, utils.post_processing],
    'FL': [
        utils.standardize_data,
        aggregate_outbreaks.preclean_FL,
        aggregate_outbreaks.fill_outbreak_status_FL,
        aggregate_outbreaks.drop_null_row_FL,
        aggregate_outbreaks.fill_state_facility_type_FL,
        aggregate_outbreaks.fill_county_FL,
        aggregate_outbreaks.state_to_ctp_FL,
        aggregate_outbreaks.clear_non_nh_cms_ids_FL,
        utils.post_processing
        ],
    'GA': [utils.standardize_data, utils.post_processing],
    'HI': [utils.standardize_data, close_outbreaks.close_outbreaks, utils.post_processing],
    'IA': [utils.standardize_data, close_outbreaks.close_outbreaks, utils.post_processing],
    'ID': [utils.standardize_data, utils.post_processing],
    'IL': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows, utils.post_processing],
    'IN': [utils.standardize_data, utils.post_processing],
    'KS': [utils.standardize_data, close_outbreaks.close_outbreaks, utils.post_processing],
    'KY': [
        utils.standardize_data,
        unreset_cumulative.preclean_KY,
        unreset_cumulative.really_update_ky_2021_data,
        utils.post_processing
        ],
    'LA': [utils.standardize_data, replace_no_data.replace_no_data, utils.post_processing],
    'MA': [utils.standardize_data, utils.post_processing],
    'MD': [utils.standardize_data, utils.post_processing],
    'ME': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows, utils.post_processing],
    'MI': [utils.standardize_data, utils.post_processing],
    'MN': [utils.standardize_data, drop_no_data.drop_no_data, utils.post_processing],
    'MO': [utils.standardize_data, utils.post_processing],
    'MS': [utils.standardize_data, utils.post_processing],
    'NC': [utils.standardize_data, utils.add_ctp_id, close_outbreaks.close_outbreaks, utils.post_processing],
    'ND': [
        utils.standardize_data,
        aggregate_outbreaks.collapse_facility_rows_no_adding,
        utils.post_processing
        ],
    'NJ': [utils.standardize_data,
           close_outbreaks.close_outbreaks,
           lambda df: aggregate_outbreaks.collapse_outbreak_rows(df[df['Date'] >= 20201224]),
           aggregate_outbreaks.nj_special_aggregator,
           utils.post_processing],
    'NM': [utils.standardize_data, close_outbreaks.close_outbreaks, utils.post_processing],
    'NV': [utils.standardize_data, utils.post_processing],
    'NY': [utils.standardize_data, utils.post_processing],
    'OH': [utils.standardize_data, utils.post_processing],
    'OK': [utils.standardize_data, utils.post_processing],
    'OR': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows, utils.post_processing],
    'PA': [utils.standardize_data, replace_no_data.replace_no_data, utils.post_processing],
    'RI': [utils.standardize_data, utils.post_processing],
    'SC': [utils.standardize_data, utils.post_processing],
    'TN': [utils.standardize_data, close_outbreaks.close_outbreaks, utils.post_processing],
    'TX': [utils.standardize_data, utils.post_processing],
    'UT': [utils.standardize_data, close_outbreaks.close_outbreaks, utils.post_processing],
    'VA': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows, utils.post_processing],
    'VT': [utils.standardize_data, close_outbreaks.close_outbreaks, utils.post_processing],
    'WI': [utils.standardize_data, close_outbreaks.close_outbreaks, utils.post_processing],
    'WV': [utils.standardize_data, utils.post_processing],
    'WY': [utils.standardize_data,
           aggregate_outbreaks.collapse_outbreak_rows,
           close_outbreaks.close_outbreaks,
           aggregate_outbreaks.sum_outbreaks,
           lambda df: utils.post_processing(df, close_unknown_outbreaks=True)],
}

def cli_process_state(states, overwrite_final_gsheet=False, out_sheet_url=None, outdir=None):
    # get the source sheet
    url_df = utils.get_all_state_urls()

    if states == 'ALL':
        states = _FUNCTION_LISTS.keys()
    else:
        states = states.split(',')

    flask.current_app.logger.info('Going to process states: %s' % ','.join(states))

    for state in states:
        if state not in _FUNCTION_LISTS:
            flask.current_app.logger.error('Skipping state, no functions found: %s' % state)
            continue

        flask.current_app.logger.info('Processing state: %s' % state)
        entry_url = utils.get_entry_url(state, url_df)
        flask.current_app.logger.info('Reading entry sheet from: %s' % entry_url)

        csv_url = utils.csv_url_for_sheets_url(entry_url)
        flask.current_app.logger.info('Reading DF from URL: %s' % csv_url)
        df = pd.read_csv(csv_url)

        # apply transformation functions in order
        functions = _FUNCTION_LISTS[state]
        for func in functions:
            t1 = time()
            orig_num_rows = df.shape[0]
            df = func(df)
            resulting_num_rows = df.shape[0]
            t2 = time()
            flask.current_app.logger.info('Running %s took %.1f seconds, %d to %d rows' % (
                func.__name__, (t2 - t1), orig_num_rows, resulting_num_rows))

        if outdir:
            outfile = os.path.join(outdir, '%s_processed.csv' % state)
            flask.current_app.logger.info('Writing to local path: %s' % outfile)
            df.to_csv(outfile, index=False)
            flask.current_app.logger.info('Done.')

        if overwrite_final_gsheet and not df.empty:  # guard against errors, no empty sheet writes
            flask.current_app.logger.info('Writing to final sheet!!')
            final_url = utils.get_final_url(state, url_df)
            utils.save_to_sheet(final_url, df)

            flask.current_app.logger.info('Done.')

        if out_sheet_url:
            flask.current_app.logger.info('Writing to other sheet: %s' % out_sheet_url)
            utils.save_to_sheet(out_sheet_url, df)
            flask.current_app.logger.info('Done.')


# If use_local_files is true, will read the outputs from "{outdir}/*_processed.csv" instead of sheets
def cli_check_state(states, outdir=None, use_local_files=False):
    # get the source sheet
    url_df = utils.get_all_state_urls()

    if states != 'ALL':
        states = states.split(',')
        url_df = url_df.loc[url_df.State.isin(states)]

    for i, row in url_df.iterrows():
        state = row.State

        if use_local_files:
            local_path = os.path.join(outdir, '%s_processed.csv' % state)
            flask.current_app.logger.info(
                'Checking state %s from local path %s' % (state, local_path))
            df = pd.read_csv(local_path)
        else:
            url = utils.csv_url_for_sheets_url(row.Final)
            flask.current_app.logger.info('Checking state %s from url %s' % (state, url))
            df = pd.read_csv(url)

        data_quality_checks.check_data_types(df)
        errors_df = data_quality_checks.do_quality_checks(df)
        cume_check_df = check_cumulative.check_cumulative(df)

        if not cume_check_df.empty and outdir:
            flask.current_app.logger.info('Writing cumulative errors for state %s' % state)
            outfile = os.path.join(outdir, '%s_decreased_cumulative.csv' % state)
            cume_check_df.to_csv(outfile, index=False)

        dupes = errors_df[errors_df['error'] != 'duplicate date data']
        date_dupes = errors_df[errors_df['error'] == 'duplicate date data']
        
        if not dupes.empty and outdir:    
            flask.current_app.logger.info('Writing duplicate errors for state %s' % state)
            outfile = os.path.join(outdir, '%s_dupes.csv' % state)
            dupes.to_csv(outfile, index=False)
        if not date_dupes.empty and outdir:
            flask.current_app.logger.info('Writing duplicate date data for state %s' % state)
            outfile = os.path.join(outdir, '%s_date_data_dupes.csv' % state)
            date_dupes.to_csv(outfile, index=False)

"""
The main "processing" module.
"""

import os

import flask
import numpy as np
import pandas as pd
from time import time

from app.api import utils, ltc, aggregate_outbreaks, close_outbreaks, data_quality_checks, \
    check_cumulative, unreset_cumulative, fill_missing_dates


_FUNCTION_LISTS = {
    'AR': [utils.standardize_data, close_outbreaks.close_outbreaks],
    'CA': [
        utils.standardize_data,
        lambda df: aggregate_outbreaks.collapse_facility_rows_no_adding(
            df, restrict_facility_types=True),
        ],
    'CO': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'DE': [utils.standardize_data, aggregate_outbreaks.collapse_facility_rows_no_adding],
    'FL': [
        utils.standardize_data,
        aggregate_outbreaks.preclean_FL,
        aggregate_outbreaks.fill_state_facility_type_FL,
        aggregate_outbreaks.collapse_outbreak_rows,
        aggregate_outbreaks.postclean_FL,
        ],
    'GA': [utils.standardize_data],
    'HI': [
        utils.standardize_data,
        close_outbreaks.close_outbreaks,
        ],
    'IA': [
        utils.standardize_data,
        close_outbreaks.close_outbreaks,
        ],
    'IL': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'KS': [
        utils.standardize_data,
        close_outbreaks.close_outbreaks,
        ],
    'KY': [
        utils.standardize_data,
        unreset_cumulative.preclean_KY,
        unreset_cumulative.really_update_ky_2021_data,
        ],
    'ME': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'MI': [utils.standardize_data],
    'MN': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'NC': [utils.standardize_data, close_outbreaks.close_outbreaks],
    'ND': [
        utils.standardize_data,
        lambda df: aggregate_outbreaks.collapse_outbreak_rows(df, add_outbreak_and_cume=False),
        ],  ## CHECK THIS
    'NJ': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'NM': [utils.standardize_data, close_outbreaks.close_outbreaks],
    'OH': [utils.standardize_data],
    'OR': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'PA': [utils.standardize_data],
    'TN': [utils.standardize_data, close_outbreaks.close_outbreaks],
    'TX': [utils.standardize_data, fill_missing_dates.fill_missing_dates],
    'UT': [utils.standardize_data, close_outbreaks.close_outbreaks],
    'VA': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'VT': [
        utils.standardize_data,
        close_outbreaks.close_outbreaks,
        ],
    'WI': [utils.standardize_data, close_outbreaks.close_outbreaks],
    'WY': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
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


def cli_check_state(states, outdir=None):
    # get the source sheet
    url_df = utils.get_all_state_urls()

    if states != 'ALL':
        states = states.split(',')
        url_df = url_df.loc[url_df.State.isin(states)]

    for i, row in url_df.iterrows():
        state = row.State
        final_url = row.Final
        url = utils.csv_url_for_sheets_url(final_url)
        if row.State == 'FL':
            # hack hack hack for now LOCAL
            url = '/Users/julia/Downloads/FL_facilities_processed - FL.csv'
        flask.current_app.logger.info('Checking state %s from url %s' % (state, url))
        df = pd.read_csv(url)
        data_quality_checks.check_data_types(df)
        errors_df = data_quality_checks.do_quality_checks(df)
        if not errors_df.empty and outdir:
            flask.current_app.logger.info('Writing duplicate errors for state %s' % state)
            outfile = os.path.join(outdir, '%s_dupes.csv' % state)
            errors_df.to_csv(outfile, index=False)

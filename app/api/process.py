"""
The main "processing" module.
"""

import flask
import numpy as np
import pandas as pd
from time import time

from app.api import utils, ltc, aggregate_outbreaks, close_outbreaks, data_quality_checks, \
    check_cumulative, unreset_cumulative


_FUNCTION_LISTS = {
    'CO': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'DE': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'FL': [
        utils.standardize_data,
        aggregate_outbreaks.preclean_FL,
        aggregate_outbreaks.collapse_outbreak_rows,
        aggregate_outbreaks.postclean_FL,
        ],
    'IL': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'ME': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'MN': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'ND': [
        utils.standardize_data,
        lambda df: aggregate_outbreaks.collapse_outbreak_rows(df, add_outbreak_and_cume=False),
        ],  ## CHECK THIS
    'NJ': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'OR': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
    'WY': [utils.standardize_data, aggregate_outbreaks.collapse_outbreak_rows],
}


def get_all_state_urls():
    # TODO: move this out into something like config.py so it's not buried here
    url_link = 'https://docs.google.com/spreadsheets/d/1kBL149bp8PWd_NMFm8Gxj-jXToSNEU9YNgQs0o9tREs/gviz/tq?tqx=out:csv&sheet=State_links'
    url_df = pd.read_csv(url_link)
    return url_df


def get_entry_url(state, url_df):
    return url_df.loc[url_df.State == state].iloc[0].Entry


def get_final_url(state, url_df):
    return url_df.loc[url_df.State == state].iloc[0].Final


def cli_process_state(state, overwrite_final_gsheet=False, out_sheet_url=None, outfile=None):
    # get the source sheet
    url_df = get_all_state_urls()
    entry_url = get_entry_url(state, url_df)
    flask.current_app.logger.info('Reading entry sheet from: %s' % entry_url)

    csv_url = utils.csv_url_for_sheets_url(entry_url)
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

    if overwrite_final_gsheet:
        flask.current_app.logger.info('Writing to final sheet!!')
        final_url = get_final_url(state, url_df)
        utils.save_to_sheet(final_url, df)
        flask.current_app.logger.info('Done.')

    if out_sheet_url:
        flask.current_app.logger.info('Writing to other sheet: %s' % out_sheet_url)
        utils.save_to_sheet(out_sheet_url, df)
        flask.current_app.logger.info('Done.')

    if outfile:
        flask.current_app.logger.info('Writing to local path: %s' % outfile)
        df.to_csv(outfile, index=False)
        flask.current_app.logger.info('Done.')


def main(args_list=None):
    if args_list is None:
        args_list = sys.argv[1:]
    args = parser.parse_args(args_list)
    cli_process_state(args.state, args.write_to_final, args.write_to_other_sheet, args.local_path)


if __name__ == '__main__':
    main()

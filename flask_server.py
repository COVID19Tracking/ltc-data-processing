"""This file is the main module which contains the app.
"""
import click

from app import create_app
from decouple import config
from app.api import ltc, aggregate_outbreaks, close_outbreaks, data_quality_checks, \
    check_cumulative, unreset_cumulative, process as process_module

import config as configs

# Figure out which config we want based on the `ENV` env variable

env_config = config("ENV", cast=str, default="develop")
config_dict = {
    'production': configs.Production,
    'develop': configs.Develop,
    'testing': configs.Testing,
}

app = create_app(config_dict[env_config]())

# for production, require a real SECRET_KEY to be set
if env_config == 'production':
    assert app.config['SECRET_KEY'] != "12345", "You must set a secure SECRET_KEY"


@app.cli.command()
def deploy():
    """Run deployment tasks."""
    return  # we have no deployment tasks


@app.cli.command("check_cumulative_data")
@click.option('-o', '--outfile')
@click.option('-w', '--onlythisweek', is_flag=True)
def check_cumulative_data(outfile, onlythisweek):
    check_cumulative.cli_check_cumulative_data(outfile, onlythisweek)


@app.cli.command("process")
@click.option('--states', default='ALL', help='State abbreviations to run for, e.g. "ME,DE"')
@click.option('--overwrite-final-gsheet', is_flag=True)
@click.option('--out-sheet-url', default='',
    help='Write the processed data to the specified Google Sheet url')
@click.option('--outdir', default='',
    help='Write the processed data to a CSV file in this local directory')
def process(states, overwrite_final_gsheet, out_sheet_url, outdir):
    """Process all data from the entry sheet in the input states as defined in the Sheet of Sheets.

    States is expected to be a comma-separated list of state abbreviations like "ME,DE". If this
    option isn't present, the functions will be run for every single state that
    has any scripts defined.

    The processing functions applied to each state are defined in app/api/process.py. 
    The resulting output can be saved to the final sheet defined in the Sheet of Sheets
    (--write-to-gsheets), to a specified sheet url (--out-sheet-url), or as a CSV to a local
    directory (--outdir).
    """
    process_module.cli_process_state(states, overwrite_final_gsheet=overwrite_final_gsheet,
        out_sheet_url=out_sheet_url, outdir=outdir)


@app.cli.command("check")
@click.option('--states', default='ALL', help='State abbreviations to run for, e.g. "ME,DE"')
@click.option('--outdir', default='',
    help='Write the erroring rows to a CSV file in this local directory')
def check(states, outdir):
    """Checks all data from the final sheet in the input states as defined in the Sheet of Sheets.

    States is expected to be a comma-separated list of state abbreviations like "ME,DE". If this
    option isn't present, checks will be run for every single state that has any scripts defined.

    The resulting output if any duplicate outbreak rows exist are saved as a CSV to a local
    directory (--outdir).
    """
    process_module.cli_check_state(states, outdir=outdir)

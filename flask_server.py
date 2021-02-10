"""This file is the main module which contains the app.
"""
import click

from app import create_app
from decouple import config
from app.api import ltc, aggregate_outbreaks, close_outbreaks, data_quality_checks

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


@app.cli.command("aggregate_outbreaks")
@click.option('-o', '--outfile')
@click.option('--write-to-sheet', is_flag=True)
@click.argument("url")
def cli_aggregate_outbreaks(outfile, url, write_to_sheet):
    aggregate_outbreaks.cli_aggregate_outbreaks(outfile, url, write_to_sheet=write_to_sheet)


@app.cli.command("close_outbreaks")
@click.option('-o', '--outputdir')
def cli_close_outbreaks(outputdir):
    close_outbreaks.cli_close_outbreaks_nm_ar(outputdir)


@app.cli.command("quality_checks")
@click.option('-o', '--outfile')
@click.argument("url")
def cli_close_outbreaks(outfile, url):
    data_quality_checks.cli_quality_checks(outfile, url)


@app.cli.command("check_data_types")
@click.argument("url")
def cli_check_data_types(url):
    data_quality_checks.cli_check_data_types(url)

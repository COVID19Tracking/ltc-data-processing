"""This file is the main module which contains the app.
"""
import click

from app import create_app
from decouple import config
from app.api import ltc

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
@click.argument("url")
def cli_aggregate_outbreaks(outfile, url):
    ltc.cli_aggregate_outbreaks(outfile, url)


@app.cli.command("close_outbreaks")
@click.option('-o', '--outfile')
@click.argument("url")
def cli_close_outbreaks(outfile, url):
    ltc.cli_close_outbreaks_nm_ar(outfile, url)


@app.cli.command("quality_checks")
@click.option('-o', '--outfile')
@click.argument("url")
def cli_close_outbreaks(outfile, url):
    ltc.cli_quality_checks(outfile, url)


@app.cli.command("check_data_types")
@click.argument("url")
def cli_check_data_types(url):
    ltc.cli_check_data_types(url)

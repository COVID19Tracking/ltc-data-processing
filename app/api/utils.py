import click
import re


def csv_url_for_sheets_url(url):
    """extract the parameters from a google docs url and formulate a CSV export url"""
    m = re.search('.*\/d\/(.*)\/edit.*#gid=(.*)', url)
    if m:
        key = m.group(1)
        gid = m.group(2)
        return f"https://docs.google.com/spreadsheets/d/{key}/export?format=csv&gid={gid}"
    else:
        click.echo('Invalid Google Sheets URL')
        raise click.Abort()

import click
import re
import gspread
import datetime

from gspread import Worksheet, WorksheetNotFound
from gspread.utils import finditem


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


def gid_for_sheets_url(url):
    """extract just the gid (worksheet id) from a google docs url"""
    m = re.search('.*#gid=(\\d+)$', url)
    if m:
        return m.group(1)
    else:
        click.echo('Invalid Google Sheets URL')
        raise click.Abort()


def save_to_sheet(target_sheet_url, df):
    """save df to the google doc at target_sheet_url. The data will always be saved to the specified
    tab within the sheet (specified within the url), and the old contents of the tab will be saved to a backup
    tab before being replaced.

    Requires authentication. You must have the client secret installed in `~/.config/gspread/credentials.json`.
    This function will open a web browser on first run to authenticate the user."""
    gc = gspread.oauth()
    sh = gc.open_by_url(target_sheet_url)

    # we need to find the target worksheet/tab. gspread doesn't provide a way to lookup tabs by id (just index or name)
    # so we'll repurpose its usual logic to do it ourselves
    target_gid = gid_for_sheets_url(target_sheet_url)
    sheet_data = sh.fetch_sheet_metadata()
    try:
        item = finditem(
            lambda x: str(x['properties']['sheetId']) == target_gid,
            sheet_data['sheets'],
        )
        ws = Worksheet(sh, item['properties'])
    except (StopIteration, KeyError):
        raise WorksheetNotFound(target_gid)

    ws_name = ws.title

    # make a backup of the target worksheet and then clear its contents
    ws.duplicate(2, new_sheet_name=f"{ws_name}_backup_{datetime.datetime.today().strftime('%Y%m%d%H%M%S')}")
    ws.clear()

    # turn the data into a 2D array with column headers
    data = [df.columns.to_list()]
    data.extend(df.fillna('').values.tolist())

    # post it to the sheet
    ws.update('A:ZZ', data)

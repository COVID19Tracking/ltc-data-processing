import click
import re
import gspread
import json
import pandas
import numpy
from numpyencoder import NumpyEncoder

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


class GsheetsEncoder(NumpyEncoder):
    """custom serializer that extends NumpyEncoder to turn pandas NaN values into empty strings"""
    def default(self, obj):
        if isinstance(obj, pandas._libs.missing.NAType):
            return ''
        else:
            return NumpyEncoder.default(self, obj)


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
    ws.clear()  # clear target worksheet contents

    # turn the data into a 2D array with column headers
    data = [df.columns.to_list()]
    # remove pure NaN values from the 2D array
    nans_removed = [[j if not (isinstance(j, float) and numpy.isnan(j)) else '' for j in i] for i in df.values.tolist()]
    data.extend(nans_removed)

    # encode the data using our encoder so it's serializable and can be posted to gsheets
    json_data = json.dumps(data, cls=GsheetsEncoder)

    # turn it back into python objects and post it to the sheet
    ws.update('A:ZZ', json.loads(json_data))

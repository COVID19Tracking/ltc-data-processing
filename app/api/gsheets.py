import click
import re
import gspread
import datetime


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


def save_to_sheet(target_sheet_url, df):
    """save df to the google doc at target_sheet_url. The data will always be saved to the first
    tab within the sheet, and the old contents of the tab will be saved to a backup tab before being replaced.

    Requires authentication. You must have the client secret installed in `~/.config/gspread/credentials.json`.
    This function will open a web browser on first run to authenticate the user."""
    gc = gspread.oauth()
    sh = gc.open_by_url(target_sheet_url)
    ws = sh.get_worksheet(0)  # always get the first tab
    ws_name = ws.title

    # make a backup of the target sheet and then clear its contents
    ws.duplicate(2, new_sheet_name=f"{ws_name}_backup_{datetime.datetime.today().strftime('%Y%m%d%H%M%S')}")
    ws.clear()

    # turn the data into a 2D array and add the column headers
    data = df.fillna('').values.tolist()
    data.insert(0, df.columns.to_list())

    # post it to the sheet
    ws.update('A:ZZ', data)

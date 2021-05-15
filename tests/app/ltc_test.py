import pytest
import filecmp
import os
import tempfile
import shutil

import pandas as pd

import flask_server
from app.api.close_outbreaks import close_outbreaks
from app.api.fill_missing_dates import fill_missing_dates
from app.api.replace_no_data import replace_no_data

@pytest.fixture(scope="session", autouse=True)
def app_context():
    with flask_server.app.app_context():
        yield

def test_close_outbreaks():
    test_csv = "tests/app/fixtures/close_outbreak_nm_example.csv"
    df = pd.read_csv(test_csv)
    closed = close_outbreaks(df)

    temp_file = tempfile.NamedTemporaryFile(delete=False)

    closed.to_csv(temp_file.name, index = False)
    assert filecmp.cmp(temp_file.name, "tests/app/fixtures/expected_close_outbreak_nm_example.csv")

    os.remove(temp_file.name)

def test_fill_in_missing_dates_non_thursday():
    test_csv = "tests/app/fixtures/state_with_missing_date_not_a_thursday.csv"
    df = pd.read_csv(test_csv)
    filled = fill_missing_dates(df)

    temp_file = tempfile.NamedTemporaryFile(delete=False)

    filled.to_csv(temp_file.name, index = False)
    assert filecmp.cmp(temp_file.name, "tests/app/fixtures/expected_state_with_missing_dates.csv")

    os.remove(temp_file.name)

def test_no_data():
    test_csv = "tests/app/fixtures/test_no_data.csv"
    df = pd.read_csv(test_csv)
    replaced = replace_no_data(df)

    temp_file = tempfile.NamedTemporaryFile(delete=False)

    replaced.to_csv(temp_file.name, index = False)
    assert filecmp.cmp(temp_file.name, "tests/app/fixtures/expected_no_data.csv")

    os.remove(temp_file.name)

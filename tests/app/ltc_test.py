import pytest
import filecmp
import os
import tempfile
import shutil

import pandas as pd

import flask_server
from app.api.close_outbreaks import close_outbreaks
from app.api.fill_in_missing_dates import fill_in_missing_dates

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

def test_fill_in_misings_dates():
    test_csv = "tests/app/fixtures/state_with_missing_dates.csv"
    df = pd.read_csv(test_csv)
    filled = fill_in_missing_dates(df, "test_state")

    temp_file = tempfile.NamedTemporaryFile(delete=False)

    filled.to_csv(temp_file.name, index = False)
    assert filecmp.cmp(temp_file.name, "tests/app/fixtures/expected_state_with_missing_dates.csv")

    os.remove(temp_file.name)

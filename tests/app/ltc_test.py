import pytest
import filecmp
import os

import pandas as pd

from app.api.close_outbreaks import close_outbreaks

def test_close_outbreaks():
    test_csv = "tests/app/fixtures/close_outbreak_nm_example.csv"
    df = pd.read_csv(test_csv)
    closed = close_outbreaks(df)
    closed.to_csv("test_nm_closed_outbreak.csv", index=False)
    assert filecmp.cmp("test_nm_closed_outbreak.csv", "tests/app/fixtures/expected_close_outbreak_nm_example.csv")
    os.remove("test_nm_closed_outbreak.csv")

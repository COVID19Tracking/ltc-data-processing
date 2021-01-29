import pytest

"""
Tests for API endpoints
"""


def test_echo(app):
    client = app.test_client()

    resp = client.post("/api/echo", data="abc", content_type="text/csv")
    assert resp.status_code == 200
    assert resp.content_type == "text/csv; charset=utf-8"
    assert resp.data.decode("utf-8") == "abc"

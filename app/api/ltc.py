"""Registers the necessary routes for the API endpoints."""

import flask
from flask import Response

from app.api import api


@api.route('/echo', methods=['POST'])
def echo():
    """Dummy route that returns the input data unaltered"""
    payload = flask.request.data
    return Response(payload, mimetype='text/csv')

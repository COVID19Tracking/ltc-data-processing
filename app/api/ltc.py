"""Registers the necessary routes for the API endpoints.

This is a set of LTC data processing scripts, e.g.: aggregates cumulative and outbreak data for
a set of states (e.g. ME) that otherwise have multiple rows per date/facility, in a case of
several outbreaks.
"""

import io
from time import time

import flask
from flask import Response
import pandas as pd
import numpy as np

from app.api import api, utils, aggregate_outbreaks, close_outbreaks


####################################################################################################
#######################################   API endpoints    #########################################
####################################################################################################


@api.route('/echo', methods=['POST'])
def echo():
    """Dummy route that returns the input data unaltered"""
    payload = flask.request.data
    return Response(payload, mimetype='text/csv')


# Basic URL to hit as a test
@api.route('/test', methods=['GET'])
def test():
    return "Hello World"


@api.route('/aggregate-outbreaks', methods=['POST'])
def api_aggregate_outbreaks():
    payload = flask.request.data.decode('utf-8')
    df = pd.read_csv(io.StringIO(payload))
    processed_df = aggregate_outbreaks.do_aggregate_outbreaks(df)

    return Response(processed_df.to_csv(index=False), mimetype='text/csv')


@api.route('/close-outbreaks-nm-ar', methods=['POST'])
def api_close_outbreaks_nm_ar():
    payload = flask.request.data.decode('utf-8')
    df = pd.read_csv(io.StringIO(payload))
    processed_df = close_outbreaks.do_close_outbreaks_nm_ar(df)

    return Response(processed_df.to_csv(index=False), mimetype='text/csv')

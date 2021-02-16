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

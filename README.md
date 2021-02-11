# LTC Data Processing API

This repo implements an API that performs data processing tasks for COVID Tracking Project long-term care spreadsheets.

It contains various functions that receive CSV data from the sheets, process it, and output the processed data.

## Development

This project assumes you have Python 3.6. You can use `pip install -r requirements.txt` to get dependencies, but it's recommended to use Conda (more container friendly later) to create the environment with all dependencies (see `environment.yml`):
```shell
conda env create --file environment.yml
```
## Running the app

To run a local development server
```shell
export FLASK_APP=flask_server.py
flask run
```

This will start a local server at http://127.0.0.1:5000/. 

You can now POST data to http://127.0.0.1:5000/api/echo

To spin up a whole stack using docker:
```shell
docker build -t cvapi .
docker run -it -p 8000:8000 cvapi
```

## Command-line interface

Example:
```shell
flask aggregate_outbreaks "https://docs.google.com/spreadsheets/d/1r7DU4FN_spe71nMa2lsIXAXgGEBw8mwyryiDlby3Q6w/edit#gid=273523772" # outputs to STDOUT
flask aggregate_outbreaks --outfile IL.csv "https://docs.google.com/spreadsheets/d/1r7DU4FN_spe71nMa2lsIXAXgGEBw8mwyryiDlby3Q6w/edit#gid=273523772" # writes to file
flask aggregate_outbreaks --write-to-sheet "https://docs.google.com/spreadsheets/d/1r7DU4FN_spe71nMa2lsIXAXgGEBw8mwyryiDlby3Q6w/edit#gid=1101937167" "https://docs.google.com/spreadsheets/d/1r7DU4FN_spe71nMa2lsIXAXgGEBw8mwyryiDlby3Q6w/edit#gid=273523772" # publishes to google sheets
flask close_outbreaks "https://docs.google.com/spreadsheets/d/1r7DU4FN_spe71nMa2lsIXAXgGEBw8mwyryiDlby3Q6w/edit#gid=273523772"
flask quality_checks "https://docs.google.com/spreadsheets/d/1r7DU4FN_spe71nMa2lsIXAXgGEBw8mwyryiDlby3Q6w/edit#gid=273523772"
```

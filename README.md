**As of March 7, 2021 we are [no longer collecting new data](https://covidtracking.com/analysis-updates/giving-thanks-and-looking-ahead-our-data-collection-work-is-done). [Learn about available federal data](https://covidtracking.com/analysis-updates/federal-covid-data-101-how-to-find-data).**

---

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

We're moving toward a single entry point to run all functions for a particular state. Like so:

```shell
FLASK_APP=flask_server.py flask process --states CA,MD --outfile=/tmp/ca.csv --overwrite-final-gsheet
```

Somewhat deprecated examples, soon to be fully deprecated:
```shell
flask quality_checks "https://docs.google.com/spreadsheets/d/1r7DU4FN_spe71nMa2lsIXAXgGEBw8mwyryiDlby3Q6w/edit#gid=273523772"
```

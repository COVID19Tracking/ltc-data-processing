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

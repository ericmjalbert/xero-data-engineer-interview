# Xero-data-engineer-take-home-interview
Take home problem for Xero Interview; includes a simple ETL through [Airflow](https://airflow.apache.org/) and a dashboard through [Dash](https://plotly.com/dash/).

![Airflow ETL](/assets/airflow_etl.png)

![Dashboard Analytics](/assets/dash_analytics.gif)


## Usage

First, make sure you have `docker` installed 

Then run the following commands in the root of this project:
```bash
docker-compose build
docker-compose up -d
bash init_etl.sh
```

Once the `init_etl` script is completed, check the [airflow ETL](http://localhost:8080/admin/airflow/graph?dag_id=log_data_processing&execution_date=) to see the log data be processed via a batch ETL.

Once the airflow ETL is completed, check the results of the analysis on this [plotly dashboard](http://localhost:8050/). *NOTE* this dashboard will not run until the ETL has completed running once. You can check the progress using the above airflow ETL link.


## Project Structure

The project is managed via docker-compose which create 3 services:
1. A PostgreSQL Database to act as a local data warehouse
2. An Airflow scheduler to act as the ETL process for transforming the data. The UI for this can be seen at `localhost:8080`. Each task logs the SQL transformations for debugging and documentation.
3. A Dashboard (via [dash](https://plotly.com/dash/)) that will show the result of the analysis. Normally, in a production setup I'd recommend *not* rolling out a custom dashboarding solution and instead using an existing BI tool (Tableau, Periscope, Looker)

The codebase is divided into 4 main folders:
1. `etl`: This is the Airflow ETL component. It manages the tasks that transform and load the log data into Postgres.
2. `xero_dash`: This is the visualization component. It holds the business logic and graphing code for the analysis. It is written as it's own python module.
3. `exploration`: This is the first pass exploration of the dataset. It serves as a log of the workflow/thought process of solving the problem. This folder exists mostly to help the efforts of reproducible research.
    * This can be run from the root folder via: `venv/bin/python exploration/explore_data.py`
4. `libraries`: These are reusable code snippets between the different services. Currently it holds database querying code.


## Development Workflow
For most dev work, it should be done within the docker-compose setup so that it best matches what would be deployed to production. This process has been setup so that every file changes will refresh `xero_airflow` and `xero_dashboard` so it will allow for a fast feedback loop.


For dev work that cannot be done within the docker containers, you need to have the virtualenv setup:

```bash
python -m virtualenv venv --python python3
venv/bin/pip install pip-tools
venv/bin/python -m piptools compile
venv/bin/pip install -r requirements.txt
```

### Packages Management

We use (pip-tools)[https://github.com/jazzband/pip-tools] to manage packaged. This means that to add a new package it needs to be added to `requirements.in` and then we need to compile a new `requirements.txt`:
```bash
venv/bin/python -m piptools compile
```

then make sure you redo the pip install:
```bash
venv/bin/pip install -r requirements.txt
```

## Next Steps

### Testing
* Setting this up with some mock data end-to-end test cases would be a smart next step. I avoided it here because of the time commitment to setting it up.

### Setup Prod Infrastructure
* Currently this entire project runs on local using a local Postgres. Moving forward it might make sense to persist the database on a cloud service provider (AWS RDS for example).

### Clean up temp files
* Running airflow locally dumps all the files into the projects root directory. It would be better to have this use a docker volume so that it doesn't fill the repo with noise

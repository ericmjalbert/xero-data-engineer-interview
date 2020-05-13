#!/bin/bash

# This script exists to "automatically" trigger the ETL dag when the project is started up.

DAG_ID=log_data_processing
AIRFLOW=xero_airflow

[ ! "$(docker ps -a | grep $AIRFLOW)" ] && docker-compose up -d
echo "Waiting 60 seconds for airflow and postgres to warm up"
sleep 60
echo "Unpausing $DAG_ID; this will automatically trigger it"
docker exec $AIRFLOW airflow unpause $DAG_ID

echo "Now wait ~30 seconds for $DAG_ID DAG to complete before checking out the dashboard"

#!/bin/bash

# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Install Apache Airflow
pip install apache-airflow

# Initialize the Airflow database
airflow db init

# Create Airflow user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Create symbolic link to our DAGs
ln -s $(pwd)/dags $AIRFLOW_HOME/dags

# Start Airflow webserver (in background)
airflow webserver -D

# Start Airflow scheduler (in background)
airflow scheduler -D

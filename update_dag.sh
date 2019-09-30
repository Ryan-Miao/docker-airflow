#!/bin/bash

DAG_HOME=/data/opt/airflow/docker-airflow/dags
cd $DAG_HOME
git pull --rebase origin master
docker exec -it docker-airflow_webserver_1 /bin/bash /list_dags.sh

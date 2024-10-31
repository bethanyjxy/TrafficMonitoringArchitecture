#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e



# Create Overlay Network
echo "Creating overlay network 'airflow-network'..."
docker network create --driver overlay airflow-network 

# Create Airflow Worker Service
echo "Creating Airflow Worker Service..."
docker service create \
  --name service-airflow-worker \
  --replicas 5 \
  --network airflow-network \
  --env AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@postgres:5432/airflow_db \
  trafficmonitoringarchitecture-airflow-worker

# Create Airflow Scheduler Service
echo "Creating Airflow Scheduler Service..."
docker service create \
  --name service-airflow-scheduler \
  --replicas 1 \
  --network airflow-network \
  --env AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@postgres:5432/airflow_db \
  trafficmonitoringarchitecture-airflow-scheduler

# Create Airflow Triggerer Service
echo "Creating Airflow Triggerer Service..."
docker service create \
  --name service-airflow-triggerer \
  --replicas 1 \
  --network airflow-network \
  --env AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@postgres:5432/airflow_db \
  trafficmonitoringarchitecture-airflow-triggerer

# List services
echo "Listing services..."
docker service ls 

# Scale Services (optional, can be modified as needed)
echo "Scaling Airflow services..."
docker service scale service-airflow-worker=5
docker service scale service-airflow-scheduler=1
docker service scale service-airflow-triggerer=1

echo "Setup complete."

#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

#sudo docker swarm init
#sudo docker network create --driver overlay airflow-network

# Create Airflow Worker Service
echo "Creating Airflow Worker 1 Service..."
sudo docker service create \
  --name service-airflow-worker-1\
  --replicas 5 \
  --network airflow-network \
  --env AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@p>
  trafficmonitoringarchitecture-airflow-worker-1

echo "Creating Airflow Worker 2 Service..."
sudo docker service create \
  --name service-airflow-worker-2\
  --replicas 5 \
  --network airflow-network \
  --env AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@p>
  trafficmonitoringarchitecture-airflow-worker-2

# Create Airflow Scheduler Service
echo "Creating Airflow Scheduler Service..."
sudo docker service create \
  --name service-airflow-scheduler \
  --replicas 1 \
  --network airflow-network \
  --env AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@p>
  trafficmonitoringarchitecture-airflow-scheduler

# Create Airflow Triggerer Service
echo "Creating Airflow Triggerer Service..."
sudo docker service create \
  --name service-airflow-triggerer \
  --replicas 1 \
  --network airflow-network \
  --env AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@p>
  trafficmonitoringarchitecture-airflow-triggerer

# List services
echo "Listing services..."
sudo docker service ls 

# Scale Services (can be modified as needed)
echo "Scaling Airflow services..."
sudo docker service scale service-airflow-worker-1=5
sudo docker service scale service-airflow-worker-2=5
sudo docker service scale service-airflow-scheduler=1
sudo docker service scale service-airflow-triggerer=1

echo "Setup complete."


#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Check if Docker Swarm is active
if ! docker info --format '{{.Swarm.LocalNodeState}}' | grep -q "active"; then
  echo "Initializing Docker Swarm..."
  docker swarm init
else
  echo "Docker Swarm is active."
fi

# Create Overlay Network
NETWORK_NAME="airflow-network"
if ! docker network ls | grep -q "$NETWORK_NAME"; then
  echo "Creating overlay network '$NETWORK_NAME'..."
  docker network create --driver overlay "$NETWORK_NAME"
else
  echo "Overlay network : '$NETWORK_NAME' "
fi

# Create Airflow Worker Service
echo "Creating Airflow Worker 1 Service..."
docker service create \
  --name service-airflow-worker-1\
  --replicas 5 \
  --network airflow-network \
  --env AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@postgres:5432/airflow_db \
  trafficmonitoringarchitecture-airflow-worker-1

echo "Creating Airflow Worker 2 Service..."
docker service create \
  --name service-airflow-worker-1\
  --replicas 5 \
  --network airflow-network \
  --env AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@postgres:5432/airflow_db \
  trafficmonitoringarchitecture-airflow-worker-2

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

# Scale Services (can be modified as needed)
echo "Scaling Airflow services..."
docker service scale service-airflow-worker-1=5
docker service scale service-airflow-worker-2=5
docker service scale service-airflow-scheduler=1
docker service scale service-airflow-triggerer=1

echo "Setup complete."

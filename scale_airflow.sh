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

#echo "Creating Airflow Worker 2 Service..."
#sudo docker service create \
#  --name service-airflow-worker-2\
#  --replicas 5 \
#  --network airflow-network \
#  --env AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
#  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@p>
#  trafficmonitoringarchitecture-airflow-worker-2

# List services
echo "Listing services..."
sudo docker service ls 

# Scale Services (can be modified as needed)
echo "Scaling Airflow services..."
sudo docker service scale service-airflow-worker-1=5
#sudo docker service scale service-airflow-worker-2=5


echo "Setup complete."


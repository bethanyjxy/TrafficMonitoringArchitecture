#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

#sudo docker swarm init
#sudo docker network create --driver overlay airflow-network

# Create Airflow Worker Service
echo "Creating Airflow Worker 1 Service..."
sudo docker service create \
  --name service-airflow-worker-1 \
  --replicas 5 \
  --network airflow-network \
  --env AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@postgres-server/airflow_db \
  --env AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://traffic_admin:traffic_pass@postgres-server/airflow_db \
  --env AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0 \
  --env AIRFLOW__CORE__LOAD_EXAMPLES=false \
  --env AIRFLOW__API__AUTH_BACKENDS='airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session' \
  --env AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true \
  trafficmonitoringarchitecture-airflow-worker-1

#echo "Creating Airflow Worker 2 Service..."
#sudo docker service create \
#  --name service-airflow-worker-2\
#  --replicas 5 \
#  --network airflow-network \
#  --env AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
#  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@p>
#  trafficmonitoringarchitecture-airflow-worker-2

sudo docker service create \
  --name service-airflow-scheduler \
  --replicas 1 \
  --network airflow-network \
  --env AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@postgres-server/airflow_db \
  trafficmonitoringarchitecture-airflow-scheduler

sudo docker service create \
  --name service-airflow-trigger \
  --replicas 1 \
  --network airflow-network \
  --env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://traffic_admin:traffic_pass@postgres-server/airflow_db \
  trafficmonitoringarchitecture-airflow-triggerer


# List services
echo "Listing services..."
sudo docker service ls 

# Scale Services (can be modified as needed)
echo "Scaling Airflow services..."
sudo docker service scale service-airflow-worker-1=5
#sudo docker service scale service-airflow-worker-2=5


echo "Setup complete."


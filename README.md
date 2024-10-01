# TrafficMonitoringArchitecture

run postgres and create db for airflow

docker-compose up -d postgres

docker exec -it postgres psql -U traffic_admin -d postgres -c "CREATE DATABASE airflow_db;"

Hadoop
docker exec -it hadoop-namenode bash
hdfs dfs -mkdir -p /user/hadoop/traffic_data
hdfs dfs -chown hadoop:supergroup /user/hadoop/traffic_data
hdfs dfs -chmod 755 /user/hadoop/traffic_data


Spark semilink
docker exec -it --user root <container_name> /bin/bash
rm -rf /opt/spark
ln -s /opt/spark-3.1.1-bin-hadoop2.7 /opt/spark

ls -l /opt/spark
ls /opt/spark/bin/

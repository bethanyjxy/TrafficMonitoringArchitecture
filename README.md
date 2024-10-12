# TrafficMonitoringArchitecture



Up ONLY POSTGRES Container
1. 
docker-compose up -d postgres

2. 
docker exec -it postgres psql -U traffic_admin -d postgres -c "CREATE DATABASE airflow_db;"



Enter Hadoop Container to grant access 
1. 
docker exec -it hadoop-namenode bash
2. 
hdfs dfs -mkdir -p /user/hadoop/traffic_data
3. 
hdfs dfs -chown hadoop:supergroup /user/hadoop/traffic_data



##### For Hadoop to Spark
1. Copy to container


2. Create directory and put in hdfs
docker exec -it hadoop-namenode bash
hdfs dfs -mkdir -p /user/hadoop/historical
hdfs dfs -put /historical/*.csv /user/hadoop/historical/
hdfs dfs -ls /user/hadoop/historical/ 


    

Enter Postgre and Check data
1. 
docker exec -it postgres psql -U traffic_admin -d traffic_db



( Nth much)
Spark Checks

ls -l /opt/spark
ls /opt/spark/bin/



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




Enter airflow
1.  
docker exec -it airflow-worker /bin/bash
2. 
python3 app/kafka/producer/producer.py

3. 
python3 app/kafka/consumer/kafka_incidents_consumer.py &
python3 app/kafka/consumer/kafka_images_consumer.py &
python3 app/kafka/consumer/kafka_speedbands_consumer.py &
python3 app/kafka/consumer/kafka_vms_consumer.py 


Add another terminal for spark

Enter airflow container
1.  
docker exec -it airflow-worker /bin/bash
2. 
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
    --jars /opt/spark/jars/postgresql-42.2.18.jar \
    app/spark/realtime/postgres_stream.py

    

Enter Postgre and Check data
1. 
docker exec -it postgres psql -U traffic_admin -d traffic_db



( Nth much)
Spark Checks

ls -l /opt/spark
ls /opt/spark/bin/


##### For Hadoop to Spark
1. Copy to container
docker cp app/historical_data hadoop-namenode:historical

2. Create directory and put in hdfs
docker exec -it hadoop-namenode bash
hdfs dfs -mkdir -p /user/hadoop/historical
hdfs dfs -put /historical/*.csv /user/hadoop/historical/
hdfs dfs -ls /user/hadoop/historical/ 

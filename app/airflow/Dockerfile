FROM apache/airflow:2.10.2

# Install Python packages
RUN pip install --no-cache-dir Flask psycopg2-binary dash dash-bootstrap-components plotly kafka-python kafka confluent-kafka folium pyspark==3.1.1 hdfs pandas apache-airflow requests numpy jsons

USER root

# Install Java (openjdk-17-jdk-headless)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    apt-get install -y wget && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz | tar -xz -C /opt/ && \
    ln -s /opt/spark-3.1.1-bin-hadoop2.7 /opt/spark && \
    mkdir -p /opt/spark/jars && \
    wget https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -P /opt/spark/jars/ && \
    rm -rf /var/lib/apt/lists/*

# Set SPARK_HOME and update the PATH
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Create the /opt/spark/jars directory for additional Spark jars
RUN mkdir -p /opt/spark/jars

USER airflow


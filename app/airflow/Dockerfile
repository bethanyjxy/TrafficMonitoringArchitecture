FROM apache/airflow:2.10.2

# Install Python packages
RUN pip install --no-cache-dir Flask psycopg2-binary dash dash-bootstrap-components plotly kafka-python kafka confluent-kafka folium pyspark==3.1.1 hdfs pandas apache-airflow requests numpy jsons apache-airflow-providers-postgres apache-airflow-providers-apache-spark

USER root

# Install Java 8
RUN apt-get update && \
    apt-get install -y wget && \
    mkdir -p /opt/java && \
    wget -q https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u392-b08/OpenJDK8U-jdk_x64_linux_hotspot_8u392b08.tar.gz && \
    tar -xzf OpenJDK8U-jdk_x64_linux_hotspot_8u392b08.tar.gz -C /opt/java && \
    rm OpenJDK8U-jdk_x64_linux_hotspot_8u392b08.tar.gz && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/opt/java/jdk8u392-b08
ENV PATH="$JAVA_HOME/bin:$PATH"
# Install Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz | tar -xz -C /opt/ && \
    ln -s /opt/spark-3.1.1-bin-hadoop2.7 /opt/spark && \
    ls -l /opt/spark/bin && \  
    chmod +x /opt/spark/bin/spark-submit && \ 
    mkdir -p /opt/spark/jars && \
    wget https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -P /opt/spark/jars/ && \
    rm -rf /var/lib/apt/lists/*

# Set SPARK_HOME and update the PATH
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
RUN chmod +x /opt/spark-3.1.1-bin-hadoop2.7/bin/spark-class

# Switch to non-root user
USER airflow
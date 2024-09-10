FROM python:3.11-slim

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

# Download and include the Spark Kafka package
RUN wget https://repos.spark-packages.org/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.1/spark-sql-kafka-0-10_2.12-3.1.1.jar \
    -P /opt/spark/jars/
    
EXPOSE 5000

ENV FLASK_APP=app.py
ENV FLASK_ENV=development

CMD ["python","app.py"]


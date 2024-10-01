FROM python:3.11-slim

WORKDIR /app

COPY . /app


RUN pip install --no-cache-dir -r requirements.txt

ENV SPARK_CLASSPATH="/opt/spark/jars/postgresql-42.2.18.jar"

EXPOSE 5000

ENV FLASK_APP=app.py
ENV FLASK_ENV=development

CMD ["python","app.py"]


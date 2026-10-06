# PySpark 3.5.1 + Java 17 + the Python libraries used in this project
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    numpy==1.26.4 \
    pandas==2.2.2 \
    kafka-python==2.0.2 \
    hdfs==2.7.3 \
    pymysql==1.1.1

WORKDIR /app

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt update && apt-get install -y --no-install-recommends \
      openjdk-11-jdk-headless \
      python3 \
      python3-pip \
      wget \
      tzdata \
      zip \
      gcc \
      heimdal-dev \
      pkg-config \
      python3-dev \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/

ARG AIRFLOW_VERSION=2.6.3
ARG SPARK_VERSION=3.4.1
ARG PYTHON_VERSION=3.10

ENV SPARK_HOME /usr/local/lib/python${PYTHON_VERSION}/dist-packages/pyspark
RUN mkdir -p ${SPARK_HOME}/jars && \
    wget -O ${SPARK_HOME}/jars/hadoop-azure-3.3.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.1/hadoop-azure-3.3.1.jar

ENV PYSPARK_PYTHON /usr/bin/python3

WORKDIR /opt/airflow

COPY requirements.txt .
ARG CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
RUN pip install --requirement requirements.txt --constraint ${CONSTRAINTS}

ENV AIRFLOW__CORE__LOAD_EXAMPLES false
ENV AIRFLOW_HOME /opt/airflow
COPY . .

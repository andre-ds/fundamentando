# Base Image
FROM ubuntu:latest
MAINTAINER Andre


# Arguments Airflow
ARG AIRFLOW_HOME=/opt/airflow
ARG AIRFLOW_VERSION=2.3.3
# Arguments Java
ARG JAVA_VERSION=11.0.16
# Spark
ARG SPARK_VERSION=spark-3.3.0
ARG SPARK_FILE=spark-3.3.0-bin-hadoop3
# Pytho
ARG PYTHON_VERSION=python3.9

# Export the environment
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Install Dependencies
RUN apt update \
    && apt install curl -yqq \
    && apt install nano -yqq \
    && apt install wget -yqq \
    && apt install python3 -yqq \
    && apt install pip -yqq

# Set timezone:
ENV TZ=America/Sao_Paulo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install Java
RUN apt update \
    && apt install default-jdk -yqq

# Install Pyspark
RUN mkdir -p /opt/spark
RUN wget https://dlcdn.apache.org/spark/${SPARK_VERSION}/${SPARK_FILE}.tgz
RUN tar -xvf /${SPARK_FILE}.tgz -C /opt/spark/
# Export the environment
ENV SPARK_HOME=/opt/spark/${SPARK_FILE}
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PATH=$PATH:$JAVA_HOME/jre/bin

ENV HADOOP_CONF_DIR=/opt/spark/${SPARK_FILE}/etc/hadoop/
ENV YARN_CONF_DIR=/opt/spark/${SPARK_FILE}/etc/hadoop/

# Install apache airflow with subpackages
RUN apt update \
    && apt install build-essential -yqq \
    && apt install libpq-dev -yqq

RUN pip install apache-airflow[aws,spark,postgres]==${AIRFLOW_VERSION}

# Install Data Science packages
RUN pip install pandas \
    && pip install yfinance

# Install Postgres
#RUN apt install postgresql postgresql-contrib -yqq \
#    &&  pip install psycopg2

# Expose ports (just to indicate that this container needs to map port)
EXPOSE 8080

# Execute CMD
RUN echo "airflow db init" >> ~/.bashrc \
    && echo '''airflow users create --role Admin --username airflow --password airflow --email airflow@airflow.com --firstname airflow --lastname airflow''' >> ~/.bashrc \
    && echo  "airflow scheduler &> /dev/null &"  >> ~/.bashrc \
    && echo  "airflow webserver &> /dev/null &"  >> ~/.bashrc
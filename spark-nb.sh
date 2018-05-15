#!/usr/bin/env bash
export SPARK_HOME=/home/ubuntu/git/labs-re-rec-v2/spark-2.3.0-bin-hadoop2.7



export PYSPARK_PYTHON=python3
export PATH=$SPARK_HOME:$PATH
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
MYSQLJAR=/home/ubuntu/git/labs-re-rec-v2/libs/mysql-connector-java-5.1.4.jar

DRIVER_MEMORY=10g
EXECUTOR_MEMORY=10g

PYSPARK_DRIVER_PYTHON=jupyter-notebook ${SPARK_HOME}/bin/pyspark --driver-memory ${DRIVER_MEMORY} --executor-memory ${EXECUTOR_MEMORY} --jars $MYSQLJAR --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.2

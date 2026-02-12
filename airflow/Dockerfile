FROM apache/airflow:2.8.1

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends docker.io curl openjdk-17-jre-headless procps \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ARG SPARK_VERSION=3.4.2
ARG HADOOP_VERSION=3

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
  | tar -xz -C /opt \
  && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark

RUN mkdir -p /home/airflow/.ivy2 \
  && /opt/spark/bin/spark-submit --master local \
     --conf spark.jars.ivy=/home/airflow/.ivy2 \
     --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,mysql:mysql-connector-java:8.0.33 \
     --class org.apache.spark.examples.SparkPi \
     /opt/spark/examples/jars/spark-examples_2.12-3.4.2.jar 1

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-eng",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="cassandra_to_kafka_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="* * * * *",
    catchup=False,
    default_args=default_args,
    tags=["spark", "cassandra", "kafka"],
) as dag:

    cassandra_to_kafka = SparkSubmitOperator(
        task_id="cassandra_to_kafka",
        application="/opt/spark-apps/cassandra_to_kafka.py",
        conn_id="spark_default",
        conf={
            "spark.jars.ivy": "/home/airflow/.ivy2",
            "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.LocalFileSystem",
        },
        repositories="https://repo1.maven.org/maven2",
        packages="com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2",
    )

    kafka_to_mysql = SparkSubmitOperator(
        task_id="kafka_to_mysql",
        application="/opt/spark-apps/kafka_to_mysql.py",
        conn_id="spark_default",
        conf={
            "spark.jars.ivy": "/home/airflow/.ivy2",
            "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.LocalFileSystem",
        },
        repositories="https://repo1.maven.org/maven2",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,mysql:mysql-connector-java:8.0.33",
    )

    cassandra_to_kafka >> kafka_to_mysql

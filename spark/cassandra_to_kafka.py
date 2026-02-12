from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, struct, to_json
from datetime import datetime

# ---------------- SPARK ----------------
spark = (
    SparkSession.builder.appName("cassandra-to-kafka")
    .config(
        "spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0"
    )
    .getOrCreate()
)

spark.conf.set("spark.cassandra.connection.host", "cassandra")
spark.conf.set("spark.cassandra.connection.port", "9042")

TOPIC = "cassandra-events"


# ---------------- TIME HELPERS ----------------
def normalize_datetime(t):
    if t is None:
        return None
    if isinstance(t, str):
        return datetime.fromisoformat(t.replace("Z", ""))
    return t


def get_latest_time_cassandra():
    df = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="tracking", keyspace="recruitment")
        .load()
    )
    return normalize_datetime(df.agg({"ts": "max"}).collect()[0][0])


def get_latest_time_kafka():
    """
    Airflow will pass last_processed_ts via XCom later.
    For now, default to epoch.
    """
    return datetime(1998, 1, 1, 0, 0, 0)


# ---------------- MAIN TASK ----------------
def run():
    cassandra_time = get_latest_time_cassandra()
    kafka_time = get_latest_time_kafka()

    print("Cassandra latest:", cassandra_time)
    print("Kafka latest:", kafka_time)

    if not cassandra_time or cassandra_time <= kafka_time:
        print("No new data to send")
        return

    df = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="tracking", keyspace="recruitment")
        .load()
        .where(col("ts") > kafka_time)
        .select(
            "ts",
            "job_id",
            "custom_track",
            "bid",
            "campaign_id",
            "group_id",
            "publisher_id",
        )
        .filter(col("job_id").isNotNull())
    )

    if df.rdd.isEmpty():
        print("No new rows")
        return

    out_df = (
        df.withColumn("bid", coalesce(col("bid").cast("double"), lit(0.0)))
        .withColumn("source", lit("cassandra"))
        .select(
            col("job_id").cast("string").alias("key"),
            to_json(
                struct(
                    col("ts"),
                    col("job_id"),
                    col("custom_track"),
                    col("bid"),
                    col("campaign_id"),
                    col("group_id"),
                    col("publisher_id"),
                    col("source"),
                )
            ).alias("value"),
        )
    )

    out_df.write.format("kafka").option(
        "kafka.bootstrap.servers", "kafka:9092"
    ).option("topic", TOPIC).save()

    print(f"Sent {out_df.count()} events to Kafka")

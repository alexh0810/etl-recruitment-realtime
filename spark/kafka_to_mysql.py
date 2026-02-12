from datetime import datetime

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, coalesce
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


# ---------------- SPARK ----------------
spark = (
    SparkSession.builder.appName("kafka-to-mysql")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,"
        "mysql:mysql-connector-java:8.0.33",
    )
    .getOrCreate()
)

MYSQL_URL = "jdbc:mysql://mysql:3306/dw?serverTimezone=UTC"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"

TOPIC = "cassandra-events"


# ---------------- KAFKA SCHEMA ----------------
EVENT_SCHEMA = StructType(
    [
        StructField("ts", StringType(), True),
        StructField("job_id", IntegerType(), True),
        StructField("custom_track", StringType(), True),
        StructField("bid", DoubleType(), True),
        StructField("campaign_id", IntegerType(), True),
        StructField("group_id", IntegerType(), True),
        StructField("publisher_id", IntegerType(), True),
        StructField("source", StringType(), True),
    ]
)


# ---------------- METRICS ----------------
def calculating_clicks(df):
    clicks = df.filter(df.custom_track == "click").na.fill(
        {"bid": 0, "job_id": 0, "publisher_id": 0, "group_id": 0, "campaign_id": 0}
    )
    clicks.createOrReplaceTempView("clicks")
    return spark.sql(
        """
        SELECT job_id,
               date(ts) AS date,
               hour(ts) AS hour,
               publisher_id,
               campaign_id,
               group_id,
               avg(bid) AS bid_set,
               count(*) AS clicks,
               sum(bid) AS spend_hour
        FROM clicks
        GROUP BY job_id, date(ts), hour(ts),
                 publisher_id, campaign_id, group_id
    """
    )


def calculating_conversion(df):
    data = df.filter(df.custom_track == "conversion").na.fill(
        {"job_id": 0, "publisher_id": 0, "group_id": 0, "campaign_id": 0}
    )
    data.createOrReplaceTempView("conversion")
    return spark.sql(
        """
        SELECT job_id,
               date(ts) AS date,
               hour(ts) AS hour,
               publisher_id,
               campaign_id,
               group_id,
               count(*) AS conversions
        FROM conversion
        GROUP BY job_id, date(ts), hour(ts),
                 publisher_id, campaign_id, group_id
    """
    )


def calculating_qualified(df):
    data = df.filter(df.custom_track == "qualified").na.fill(
        {"job_id": 0, "publisher_id": 0, "group_id": 0, "campaign_id": 0}
    )
    data.createOrReplaceTempView("qualified")
    return spark.sql(
        """
        SELECT job_id,
               date(ts) AS date,
               hour(ts) AS hour,
               publisher_id,
               campaign_id,
               group_id,
               count(*) AS qualified
        FROM qualified
        GROUP BY job_id, date(ts), hour(ts),
                 publisher_id, campaign_id, group_id
    """
    )


def calculating_unqualified(df):
    data = df.filter(df.custom_track == "unqualified").na.fill(
        {"job_id": 0, "publisher_id": 0, "group_id": 0, "campaign_id": 0}
    )
    data.createOrReplaceTempView("unqualified")
    return spark.sql(
        """
        SELECT job_id,
               date(ts) AS date,
               hour(ts) AS hour,
               publisher_id,
               campaign_id,
               group_id,
               count(*) AS unqualified
        FROM unqualified
        GROUP BY job_id, date(ts), hour(ts),
                 publisher_id, campaign_id, group_id
    """
    )


def process_kafka_data(df):
    batch_updated_at = df.agg(sf.max("ts")).collect()[0][0]
    clicks = calculating_clicks(df)
    conv = calculating_conversion(df)
    qual = calculating_qualified(df)
    unqual = calculating_unqualified(df)

    return (
        clicks.join(
            conv,
            ["job_id", "date", "hour", "publisher_id", "campaign_id", "group_id"],
            "full",
        )
        .join(
            qual,
            ["job_id", "date", "hour", "publisher_id", "campaign_id", "group_id"],
            "full",
        )
        .join(
            unqual,
            ["job_id", "date", "hour", "publisher_id", "campaign_id", "group_id"],
            "full",
        )
        .withColumn("updated_at", lit(batch_updated_at))
    )


# ---------------- DIM JOB ----------------
def retrieve_company_data():
    sql = "(SELECT id AS job_id, company_id, group_id, campaign_id FROM job) j"
    try:
        return (
            spark.read.format("jdbc")
            .option("url", MYSQL_URL)
            .option("dbtable", sql)
            .option("user", MYSQL_USER)
            .option("password", MYSQL_PASSWORD)
            .option("driver", MYSQL_DRIVER)
            .load()
        )
    except Exception:
        schema = "job_id int, company_id int, group_id int, campaign_id int"
        return spark.createDataFrame([], schema=schema)


# ---------------- LOAD MYSQL ----------------
def import_to_mysql(df):
    final = (
        df.select(
            "job_id",
            "date",
            "hour",
            "publisher_id",
            "company_id",
            "campaign_id",
            "group_id",
            "unqualified",
            "qualified",
            "conversions",
            "clicks",
            "bid_set",
            "spend_hour",
            "updated_at",
        )
        .withColumn("dates", col("date").cast("string"))
        .withColumn("hours", col("hour").cast("int"))
        .withColumnRenamed("qualified", "qualified_application")
        .withColumnRenamed("unqualified", "disqualified_application")
        .withColumnRenamed("conversions", "conversion")
        .withColumn("sources", lit("Cassandra"))
    )

    final.write.format("jdbc").option("url", MYSQL_URL).option(
        "dbtable", "events"
    ).option("user", MYSQL_USER).option("password", MYSQL_PASSWORD).option(
        "driver", MYSQL_DRIVER
    ).mode("append").save()


# ---------------- TIME CHECKS ----------------
def get_mysql_latest_time():
    sql = "(SELECT MAX(updated_at) AS max_ts FROM events) t"
    df = (
        spark.read.format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", sql)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", MYSQL_DRIVER)
        .load()
    )
    t = df.collect()[0][0]
    return t or "1998-01-01 00:00:00"


# ---------------- MAIN TASK ----------------
def main():
    mysql_time = get_mysql_latest_time()
    print("MySQL latest time:", mysql_time)

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    events = (
        raw.select(sf.from_json(col("value").cast("string"), EVENT_SCHEMA).alias("v"))
        .select("v.*")
        .withColumn(
            "ts",
            coalesce(
                to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss.SSS"),
                to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss"),
            ),
        )
        .filter(col("ts").isNotNull())
        .filter(col("ts") > to_timestamp(lit(mysql_time)))
        .filter(col("job_id").isNotNull())
    )

    if events.rdd.isEmpty():
        print("No new Kafka data to process")
        return

    metrics = process_kafka_data(events)
    company = retrieve_company_data()

    final_output = (
        metrics.join(company, "job_id", "left")
        .drop(company.group_id)
        .drop(company.campaign_id)
        .na.fill(
            {
                "company_id": 0,
                "group_id": 0,
                "campaign_id": 0,
                "publisher_id": 0,
                "qualified": 0,
                "unqualified": 0,
                "conversions": 0,
                "clicks": 0,
                "bid_set": 0.0,
                "spend_hour": 0.0,
            }
        )
    )

    import_to_mysql(final_output)
    print("Kafka batch processed successfully")


if __name__ == "__main__":
    main()

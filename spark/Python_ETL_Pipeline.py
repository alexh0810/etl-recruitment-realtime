# pyspark --packages \
# com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,\
# mysql:mysql-connector-java:8.0.33

from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as sf
from pyspark.sql.functions import col, lit
import time

# ---------------- SPARK ----------------
spark = SparkSession.builder.config(
    "spark.jars.packages",
    "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,"
    "mysql:mysql-connector-java:8.0.33",
).getOrCreate()

spark.conf.set("spark.cassandra.connection.host", "cassandra")
spark.conf.set("spark.cassandra.connection.port", "9042")

MYSQL_URL = "jdbc:mysql://mysql:3306/dw?serverTimezone=UTC"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"
MYSQL_USER = "alex"
MYSQL_PASSWORD = "etlpass"


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


# ---------------- PROCESS CASSANDRA ----------------
def process_cassandra_data(df):
    batch_updated_at = df.agg(sf.max("ts")).collect()[0][0]

    clicks = calculating_clicks(df)
    conv = calculating_conversion(df)
    qual = calculating_qualified(df)
    unqual = calculating_unqualified(df)

    final = (
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
    )

    return final.withColumn("updated_at", lit(batch_updated_at))


# ---------------- DIM JOB ----------------
def retrieve_company_data():
    sql = "(SELECT id AS job_id, company_id, group_id, campaign_id FROM job) j"
    return (
        spark.read.format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", sql)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", MYSQL_DRIVER)
        .load()
    )


# ---------------- LOAD MYSQL ----------------
def import_to_mysql(df):
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
    ).withColumnRenamed("date", "dates").withColumnRenamed(
        "hour", "hours"
    ).withColumnRenamed(
        "qualified", "qualified_application"
    ).withColumnRenamed(
        "unqualified", "disqualified_application"
    ).withColumnRenamed(
        "conversions", "conversion"
    ).withColumn(
        "sources", lit("Cassandra")
    ).write.format(
        "jdbc"
    ).option(
        "url", MYSQL_URL
    ).option(
        "dbtable", "events"
    ).option(
        "user", MYSQL_USER
    ).option(
        "password", MYSQL_PASSWORD
    ).option(
        "driver", MYSQL_DRIVER
    ).mode(
        "append"
    ).save()


# ---------------- TIME CHECKS ----------------
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
    return normalize_datetime(df.agg(sf.max("ts")).collect()[0][0])


def get_mysql_latest_time():
    sql = "(SELECT MAX(updated_at) AS updated_at FROM events) t"
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
    return normalize_datetime(t) or datetime(1998, 1, 1, 0, 0, 0)


# ---------------- MAIN TASK ----------------
def main_task(mysql_time):
    df = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="tracking", keyspace="recruitment")
        .load()
        .where(col("ts") > mysql_time)
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
        print("No new data to process")
        return

    cassandra_output = process_cassandra_data(df)
    company = retrieve_company_data()

    final_output = (
        cassandra_output.join(company, "job_id", "left")
        .drop(company.group_id)
        .drop(company.campaign_id)
    )

    import_to_mysql(final_output)
    print("Batch processed successfully")


# ---------------- LOOP ----------------
while True:
    start = datetime.now()

    cassandra_time = get_latest_time_cassandra()
    mysql_time = get_mysql_latest_time()

    print("Cassandra latest:", cassandra_time, type(cassandra_time))
    print("MySQL latest:", mysql_time, type(mysql_time))

    if cassandra_time and cassandra_time > mysql_time:
        main_task(mysql_time)
    else:
        print("No new data found")

    print("Execution time:", (datetime.now() - start).total_seconds(), "seconds")
    time.sleep(5)

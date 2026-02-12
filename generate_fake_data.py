from cassandra.cluster import Cluster
import cassandra
import random
import time
from datetime import datetime, UTC
import pandas as pd
import mysql.connector

print("Starting Cassandra tracking data generator...")

# -----------------------------
# MySQL config
# -----------------------------
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3305
MYSQL_DB = "dw"
MYSQL_USER = "alex"
MYSQL_PASSWORD = "etlpass"

# -----------------------------
# Cassandra config
# -----------------------------
KEYSPACE = "recruitment"
TABLE = "tracking"

cluster = Cluster(contact_points=["127.0.0.1"], port=9042)
session = cluster.connect(KEYSPACE)


# -----------------------------
# Load dimension data
# -----------------------------
def load_jobs():
    cnx = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
    )
    df = pd.read_sql("SELECT id AS job_id, campaign_id, group_id FROM job", cnx)
    cnx.close()
    return df


def load_publishers():
    cnx = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
    )
    df = pd.read_sql("SELECT DISTINCT id AS publisher_id FROM master_publisher", cnx)
    cnx.close()
    return df


jobs_df = load_jobs()
publishers = load_publishers()["publisher_id"].tolist()

job_ids = jobs_df["job_id"].tolist()
campaign_ids = jobs_df["campaign_id"].dropna().astype(int).tolist()
group_ids = jobs_df["group_id"].dropna().astype(int).tolist()

if not (job_ids and campaign_ids and group_ids and publishers):
    raise RuntimeError("Required dimension tables are empty")

# -----------------------------
# Prepared Cassandra statement
# -----------------------------
insert_stmt = session.prepare(
    f"""
INSERT INTO {TABLE}
(create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""
)


# -----------------------------
# Event generator
# -----------------------------
def generate_events(n_events: int):
    for _ in range(n_events):
        event_time = datetime.now(UTC)

        create_time = str(cassandra.util.uuid_from_time(event_time))
        ts = event_time.strftime("%Y-%m-%d %H:%M:%S")

        session.execute(
            insert_stmt,
            (
                create_time,
                random.randint(0, 1),
                random.choice(campaign_ids),
                random.choices(
                    ["click", "conversion", "qualified", "unqualified"],
                    weights=[70, 10, 10, 10],
                )[0],
                random.choice(group_ids),
                random.choice(job_ids),
                random.choice(publishers),
                ts,
            ),
        )

    print(f"{n_events} events generated at {ts}")


# -----------------------------
# Loop
# -----------------------------
while True:
    generate_events(random.randint(1, 20))
    time.sleep(20)

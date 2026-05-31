from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("bike-recommend") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/data/*.csv")

print(f"Loaded {df.count()} rows")

# parse timestamps
df = df.withColumn("started_at_ts", F.to_timestamp("started_at"))
df = df.withColumn("ended_at_ts", F.to_timestamp("ended_at"))
df = df.filter(F.col("started_at_ts").isNotNull() & F.col("ended_at_ts").isNotNull())

# extract hours
df = df.withColumn("start_hour", F.hour("started_at_ts"))
df = df.withColumn("end_hour", F.hour("ended_at_ts"))

# count starts per (hour, user_type, station)
starts = df.groupBy("start_hour", "member_casual", "start_station_name") \
    .agg(F.count("*").alias("starts"))

# count ends per (hour, user_type, station)
ends = df.groupBy("end_hour", "member_casual", "end_station_name") \
    .agg(F.count("*").alias("ends"))

# join starts and ends
net = starts.alias("s").join(
    ends.alias("e"),
    (F.col("s.start_hour") == F.col("e.end_hour")) &
    (F.col("s.member_casual") == F.col("e.member_casual")) &
    (F.col("s.start_station_name") == F.col("e.end_station_name")),
    "outer"
)

# fill nulls and compute net inflow
net = net.withColumn("hour", F.coalesce(F.col("s.start_hour"), F.col("e.end_hour")))
net = net.withColumn("user_type", F.coalesce(F.col("s.member_casual"), F.col("e.member_casual")))
net = net.withColumn("station", F.coalesce(F.col("s.start_station_name"), F.col("e.end_station_name")))
net = net.fillna(0, subset=["starts", "ends"])
net = net.withColumn("net_inflow", F.col("ends") - F.col("starts"))
net = net.withColumn("total_activity", F.col("starts") + F.col("ends"))

# filter to stations with enough activity (at least 20 combined starts+ends at that hour)
net = net.filter(F.col("total_activity") > 20)

# for each (hour, user_type), pick the station with highest net_inflow
from pyspark.sql.window import Window

rec_window = Window.partitionBy("hour", "user_type").orderBy(F.desc("net_inflow"))
best = net.withColumn("rank", F.row_number().over(rec_window)) \
    .filter(F.col("rank") == 1) \
    .drop("rank") \
    .select("hour", "user_type", "station", "net_inflow", "starts", "ends", "total_activity") \
    .orderBy("hour", "user_type")

# also get top 5 per combo for more context
top5_window = Window.partitionBy("hour", "user_type").orderBy(F.desc("net_inflow"))
top5 = net.withColumn("rank", F.row_number().over(top5_window)) \
    .filter(F.col("rank") <= 5) \
    .drop("rank")

rows = best.collect()
top5_rows = top5.orderBy("hour", "user_type", F.desc("net_inflow")).collect()

import json
import os

output = {}

for row in rows:
    h = str(int(row.hour))
    if h not in output:
        output[h] = {}
    output[h][row.user_type] = {
        "station": row.station,
        "net_inflow": int(row.net_inflow),
        "starts": int(row.starts),
        "ends": int(row.ends)
    }

# add top 5 alternatives
top5_data = {}
for row in top5_rows:
    h = str(int(row.hour))
    ut = row.user_type
    key = f"{h}|{ut}"
    if key not in top5_data:
        top5_data[key] = []
    top5_data[key].append({
        "station": row.station,
        "net_inflow": int(row.net_inflow),
        "starts": int(row.starts),
        "ends": int(row.ends)
    })

os.makedirs("/app/output", exist_ok=True)
with open("/app/output/recommendations.json", "w") as f:
    json.dump(output, f, indent=2)

with open("/app/output/recommendations_top5.json", "w") as f:
    json.dump(top5_data, f, indent=2)

print(f"Wrote recommendations.json — {len(output)} hours with recommendations")
print(f"Wrote recommendations_top5.json — top 5 alternatives per combo")

spark.stop()

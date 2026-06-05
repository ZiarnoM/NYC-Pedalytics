from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("bike-recommend") \
    .master("spark://spark-master:7077") \
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

starts_rows = starts.collect()
ends_rows = ends.collect()

# merge in Python — no expensive Spark join
from collections import defaultdict

# key: (hour, user_type, station)
net = defaultdict(lambda: {"starts": 0, "ends": 0})

for row in starts_rows:
    key = (int(row.start_hour), row.member_casual, row.start_station_name)
    net[key]["starts"] += int(row.starts)

for row in ends_rows:
    key = (int(row.end_hour), row.member_casual, row.end_station_name)
    net[key]["ends"] += int(row.ends)

# compute scores and filter by activity
entries = []
for (hour, user_type, station), counts in net.items():
    total = counts["starts"] + counts["ends"]
    if total > 20:
        entries.append({
            "hour": hour,
            "user_type": user_type,
            "station": station,
            "score": counts["ends"] - counts["starts"],
            "starts": counts["starts"],
            "ends": counts["ends"]
        })

# group by hour + user_type, sort by score desc
from collections import defaultdict
by_combo = defaultdict(list)
for e in entries:
    by_combo[(e["hour"], e["user_type"])].append(e)

for combo in by_combo:
    by_combo[combo].sort(key=lambda x: x["score"], reverse=True)

import json
import os

output = {}
top5_data = defaultdict(dict)

for (hour, user_type), stations in by_combo.items():
    best = stations[0]
    h_str = str(hour)
    if h_str not in output:
        output[h_str] = {}
    output[h_str][user_type] = {
        "station": best["station"],
        "score": best["score"],
        "starts": best["starts"],
        "ends": best["ends"]
    }
    top5 = stations[:5]
    top5_data[h_str][user_type] = [
        {"station": s["station"], "score": s["score"], "starts": s["starts"], "ends": s["ends"]}
        for s in top5
    ]

os.makedirs("/app/output", exist_ok=True)
with open("/app/output/recommendations.json", "w") as f:
    json.dump(output, f, indent=2)

with open("/app/output/recommendations_top5.json", "w") as f:
    json.dump(dict(top5_data), f, indent=2)

print(f"Wrote recommendations.json — {len(output)} hours with recommendations")
print(f"Wrote recommendations_top5.json — top 5 alternatives per combo")

spark.stop()

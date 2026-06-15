from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("bike-recommend")
    .master("spark://spark-master:7077")
    .config("spark.executor.memory", "1500m")
    .config("spark.sql.shuffle.partitions", "400")
    .getOrCreate()
)

df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("hdfs://namenode:9000/data/*.csv")
)

# only keep what we need
df = df.select("started_at", "ended_at", "start_station_name", "end_station_name")

df = df.withColumn("started_at_ts", F.to_timestamp("started_at"))
df = df.withColumn("ended_at_ts", F.to_timestamp("ended_at"))
df = df.filter(F.col("started_at_ts").isNotNull() & F.col("ended_at_ts").isNotNull())

# filter null stations
df = df.filter(
    F.col("start_station_name").isNotNull() & F.col("end_station_name").isNotNull()
)

df = df.withColumn("start_hour", F.hour("started_at_ts"))
df = df.withColumn("end_hour", F.hour("ended_at_ts"))

print(f"Loaded {df.count():,} rows")

# count starts per (hour, station)
starts = df.groupBy("start_hour", "start_station_name").agg(
    F.count("*").alias("starts")
)

# count ends per (hour, station)
ends = df.groupBy("end_hour", "end_station_name").agg(F.count("*").alias("ends"))

starts_rows = starts.collect()
ends_rows = ends.collect()

# merge in Python
from collections import defaultdict

net = defaultdict(lambda: {"starts": 0, "ends": 0})

for row in starts_rows:
    key = (int(row.start_hour), row.start_station_name)
    net[key]["starts"] += int(row.starts)

for row in ends_rows:
    key = (int(row.end_hour), row.end_station_name)
    net[key]["ends"] += int(row.ends)

entries = []
for (hour, station), counts in net.items():
    total = counts["starts"] + counts["ends"]
    if total > 20:
        entries.append(
            {
                "hour": hour,
                "station": station,
                "score": counts["ends"] - counts["starts"],
                "starts": counts["starts"],
                "ends": counts["ends"],
            }
        )

by_hour = defaultdict(list)
for e in entries:
    by_hour[e["hour"]].append(e)
for hour in by_hour:
    by_hour[hour].sort(key=lambda x: x["score"], reverse=True)

import json, os

output = {}
top5_data = {}
for hour, stations in by_hour.items():
    best = stations[0]
    output[str(hour)] = {
        "station": best["station"],
        "score": best["score"],
        "starts": best["starts"],
        "ends": best["ends"],
    }
    top5_data[str(hour)] = [
        {
            "station": s["station"],
            "score": s["score"],
            "starts": s["starts"],
            "ends": s["ends"],
        }
        for s in stations[:5]
    ]

os.makedirs("/app/output", exist_ok=True)
with open("/app/output/recommendations.json", "w") as f:
    json.dump(output, f, indent=2)
with open("/app/output/recommendations_top5.json", "w") as f:
    json.dump(top5_data, f, indent=2)

print(f"Wrote recommendations.json — {len(output)} hours")
print(f"Wrote recommendations_top5.json — top 5 per hour")

spark.stop()

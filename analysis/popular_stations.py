from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("popular-stations") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/data/*.csv")

print(f"Loaded {df.count()} rows")

# parse timestamps and compute derived columns
df = df.withColumn("started_at_ts", F.to_timestamp("started_at"))
df = df.withColumn("ended_at_ts", F.to_timestamp("ended_at"))
df = df.withColumn("month", F.month("started_at_ts"))
df = df.withColumn("year", F.year("started_at_ts"))

# drop rows with bad timestamps
df = df.filter(F.col("started_at_ts").isNotNull() & F.col("ended_at_ts").isNotNull())

# compute starts and ends separately — no join needed
starts = df.groupBy("start_station_name", "year", "month") \
    .agg(F.count("*").alias("start_count")) \
    .orderBy("year", "month", F.desc("start_count"))

ends = df.groupBy("end_station_name", "year", "month") \
    .agg(F.count("*").alias("end_count")) \
    .orderBy("year", "month", F.desc("end_count"))

# all-time top stations by start count
alltime = df.groupBy("start_station_name") \
    .agg(F.count("*").alias("rides")) \
    .orderBy(F.desc("rides")) \
    .limit(30)

starts_rows = starts.collect()
ends_rows = ends.collect()
alltime_rows = alltime.collect()

import json
import os

# build lookup for ends: (year, month, station) -> end_count
ends_lookup = {}
for row in ends_rows:
    key = (int(row.year), int(row.month), row.end_station_name)
    ends_lookup[key] = int(row.end_count)

# build monthly top 20
by_month = {}
for row in starts_rows:
    yr = int(row.year)
    mo = int(row.month)
    key = f"{yr}-{mo:02d}"
    if key not in by_month:
        by_month[key] = []
    if len(by_month[key]) >= 20:
        continue
    ek = (yr, mo, row.start_station_name)
    end_count = ends_lookup.get(ek, 0)
    by_month[key].append({
        "station_name": row.start_station_name,
        "start_count": int(row.start_count),
        "end_count": end_count,
        "net": end_count - int(row.start_count),
        "rides": int(row.start_count)
    })

# build ends lookup for alltime
alltime_ends = {}
for row in ends_rows:
    s = row.end_station_name
    alltime_ends[s] = alltime_ends.get(s, 0) + int(row.end_count)

output = {
    "by_month": by_month,
    "alltime_top30": []
}

for row in alltime_rows:
    s = row.start_station_name
    output["alltime_top30"].append({
        "station_name": s,
        "start_count": int(row.rides),
        "end_count": alltime_ends.get(s, 0),
        "rides": int(row.rides)
    })

os.makedirs("/app/output", exist_ok=True)
with open("/app/output/popular_stations.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"Wrote popular_stations.json — {len(output['by_month'])} months, {len(output['alltime_top30'])} all-time stations")

spark.stop()

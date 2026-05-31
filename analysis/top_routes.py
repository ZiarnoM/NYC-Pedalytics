from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("top-routes") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/data/*.csv")

print(f"Loaded {df.count()} rows")

# parse timestamps
df = df.withColumn("started_at_ts", F.to_timestamp("started_at"))
df = df.filter(F.col("started_at_ts").isNotNull())

# group by station pair
routes = df.groupBy("start_station_name", "end_station_name") \
    .agg(F.count("*").alias("rides")) \
    .orderBy(F.desc("rides"))

# flag circular trips (A -> A)
routes = routes.withColumn("is_circular",
    F.col("start_station_name") == F.col("end_station_name"))

# split into one-way and circular
oneway = routes.filter(~F.col("is_circular"))
circular = routes.filter(F.col("is_circular"))

# top 50 of each
top_oneway = oneway.limit(50)
top_circular = circular.limit(30)

# collect
oneway_rows = top_oneway.collect()
circular_rows = top_circular.collect()

# also: most asymmetric routes (stations where people go one-way predominantly)
# pick routes with >1000 rides, compute A->B vs B->A ratio
paired = oneway.filter(F.col("rides") > 1000).alias("a").join(
    oneway.filter(F.col("rides") > 1000).alias("b"),
    (F.col("a.start_station_name") == F.col("b.end_station_name")) &
    (F.col("a.end_station_name") == F.col("b.start_station_name")),
    "left_outer"
).select(
    F.col("a.start_station_name").alias("station_a"),
    F.col("a.end_station_name").alias("station_b"),
    F.col("a.rides").alias("a_to_b"),
    F.coalesce(F.col("b.rides"), F.lit(0)).alias("b_to_a")
).withColumn(
    "asymmetry", F.round(F.col("a_to_b") / (F.col("a_to_b") + F.col("b_to_a")) * 100, 1)
).filter(F.col("b_to_a") > 0).orderBy(F.desc("asymmetry")).limit(15)

asym_rows = paired.collect()

import json
import os

output = {
    "top_oneway": [],
    "top_circular": [],
    "asymmetric_routes": []
}

for row in oneway_rows:
    output["top_oneway"].append({
        "from": row.start_station_name,
        "to": row.end_station_name,
        "rides": int(row.rides)
    })

for row in circular_rows:
    output["top_circular"].append({
        "station": row.start_station_name,
        "rides": int(row.rides)
    })

for row in asym_rows:
    output["asymmetric_routes"].append({
        "station_a": row.station_a,
        "station_b": row.station_b,
        "a_to_b": int(row.a_to_b),
        "b_to_a": int(row.b_to_a),
        "asymmetry_pct": float(row.asymmetry)
    })

os.makedirs("/app/output", exist_ok=True)
with open("/app/output/top_routes.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"Wrote top_routes.json — {len(output['top_oneway'])} one-way, {len(output['top_circular'])} circular, {len(output['asymmetric_routes'])} asymmetric")

spark.stop()

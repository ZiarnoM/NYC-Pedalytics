from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("trip-durations") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/data/*.csv")

print(f"Loaded {df.count()} rows")

# parse timestamps and compute duration
df = df.withColumn("started_at_ts", F.to_timestamp("started_at"))
df = df.withColumn("ended_at_ts", F.to_timestamp("ended_at"))
df = df.filter(F.col("started_at_ts").isNotNull() & F.col("ended_at_ts").isNotNull())

df = df.withColumn("duration_secs",
    F.col("ended_at_ts").cast("long") - F.col("started_at_ts").cast("long"))

# filter bad durations
df = df.filter((F.col("duration_secs") > 0) & (F.col("duration_secs") < 86400))

# 1. duration percentiles by bike type (percentile_approx = t-digest, much faster)
bike_dur = df.groupBy("rideable_type") \
    .agg(
        F.count("*").alias("rides"),
        F.round(F.avg("duration_secs") / 60, 1).alias("avg_min"),
        F.round(F.expr("percentile_approx(duration_secs, 0.5, 10000)") / 60, 1).alias("median_min"),
        F.round(F.expr("percentile_approx(duration_secs, 0.95, 10000)") / 60, 1).alias("p95_min")
    ) \
    .orderBy("rideable_type")

# 2. duration percentiles by bike type AND user type
bike_user_dur = df.groupBy("rideable_type", "member_casual") \
    .agg(
        F.count("*").alias("rides"),
        F.round(F.avg("duration_secs") / 60, 1).alias("avg_min"),
        F.round(F.expr("percentile_approx(duration_secs, 0.5, 10000)") / 60, 1).alias("median_min"),
        F.round(F.expr("percentile_approx(duration_secs, 0.95, 10000)") / 60, 1).alias("p95_min")
    ) \
    .orderBy("rideable_type", "member_casual")

# 3. overall distribution: deciles (use percentile_approx — much faster than approxQuantile)
overall = df.agg(
    F.expr("percentile_approx(duration_secs, array(0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99), 10000)").alias("deciles")
)
deciles = overall.collect()[0][0]

# collect
bike_rows = bike_dur.collect()
bike_user_rows = bike_user_dur.collect()

import json
import os

output = {
    "by_bike_type": [],
    "by_bike_and_user": [],
    "overall_percentiles_min": {
        "p10": round(deciles[0] / 60, 1) if deciles[0] else None,
        "p25": round(deciles[1] / 60, 1) if deciles[1] else None,
        "p50": round(deciles[2] / 60, 1) if deciles[2] else None,
        "p75": round(deciles[3] / 60, 1) if deciles[3] else None,
        "p90": round(deciles[4] / 60, 1) if deciles[4] else None,
        "p95": round(deciles[5] / 60, 1) if deciles[5] else None,
        "p99": round(deciles[6] / 60, 1) if deciles[6] else None,
    }
}

for row in bike_rows:
    output["by_bike_type"].append({
        "rideable_type": row.rideable_type,
        "rides": int(row.rides),
        "avg_min": float(row.avg_min),
        "median_min": float(row.median_min),
        "p95_min": float(row.p95_min)
    })

for row in bike_user_rows:
    output["by_bike_and_user"].append({
        "rideable_type": row.rideable_type,
        "member_casual": row.member_casual,
        "rides": int(row.rides),
        "avg_min": float(row.avg_min),
        "median_min": float(row.median_min),
        "p95_min": float(row.p95_min)
    })

os.makedirs("/app/output", exist_ok=True)
with open("/app/output/trip_durations.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"Wrote trip_durations.json")

spark.stop()

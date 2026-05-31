from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("bike-type-trends") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/data/*.csv")

print(f"Loaded {df.count()} rows")

# parse timestamps
df = df.withColumn("started_at_ts", F.to_timestamp("started_at"))
df = df.filter(F.col("started_at_ts").isNotNull())
df = df.withColumn("month", F.month("started_at_ts"))
df = df.withColumn("year", F.year("started_at_ts"))

# create year-month label for cleaner output
df = df.withColumn(
    "ym",
    F.concat(F.col("year").cast("string"), F.lit("-"), F.lpad(F.col("month").cast("string"), 2, "0"))
)

# group by year-month, bike type, and user type
trends = df.groupBy("ym", "rideable_type", "member_casual") \
    .agg(F.count("*").alias("rides")) \
    .orderBy("ym", "rideable_type", "member_casual")

# also compute monthly totals and e-bike percentage
monthly_totals = df.groupBy("ym") \
    .agg(
        F.count("*").alias("total_rides"),
        F.sum(F.when(F.col("rideable_type") == "electric_bike", 1).otherwise(0)).alias("electric"),
        F.sum(F.when(F.col("rideable_type") == "classic_bike", 1).otherwise(0)).alias("classic")
    ) \
    .withColumn("pct_electric", F.round(F.col("electric") / F.col("total_rides") * 100, 1)) \
    .orderBy("ym")

detail_rows = trends.collect()
summary_rows = monthly_totals.collect()

import json
import os

output = {
    "monthly_detail": [],
    "monthly_summary": []
}

for row in detail_rows:
    output["monthly_detail"].append({
        "ym": row.ym,
        "rideable_type": row.rideable_type,
        "member_casual": row.member_casual,
        "rides": int(row.rides)
    })

for row in summary_rows:
    output["monthly_summary"].append({
        "ym": row.ym,
        "total_rides": int(row.total_rides),
        "classic": int(row.classic),
        "electric": int(row.electric),
        "pct_electric": float(row.pct_electric)
    })

os.makedirs("/app/output", exist_ok=True)
with open("/app/output/bike_type_trends.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"Wrote bike_type_trends.json — {len(output['monthly_detail'])} detail rows, {len(output['monthly_summary'])} months")

spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("circular-trips") \
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

df = df.withColumn("duration_secs",
    F.col("ended_at_ts").cast("long") - F.col("started_at_ts").cast("long"))
df = df.filter((F.col("duration_secs") > 0) & (F.col("duration_secs") < 86400))

df = df.withColumn("is_circular",
    F.col("start_station_name") == F.col("end_station_name"))

df = df.withColumn("hour", F.hour("started_at_ts"))
df = df.withColumn("month", F.month("started_at_ts"))

# 1. overall circular stats
total = df.count()
circ_total = df.filter(F.col("is_circular")).count()
pct_circ = round(circ_total / total * 100, 2)

print(f"Total rides: {total:,}")
print(f"Circular: {circ_total:,} ({pct_circ}%)")

# 2. circular rate by user type
circ_by_user = df.groupBy("member_casual") \
    .agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("is_circular"), 1).otherwise(0)).alias("circular")
    ) \
    .withColumn("pct_circular", F.round(F.col("circular") / F.col("total") * 100, 2))

# 3. duration comparison: circular vs one-way
dur_comparison = df.groupBy("is_circular") \
    .agg(
        F.count("*").alias("rides"),
        F.round(F.avg("duration_secs") / 60, 1).alias("avg_min"),
        F.round(F.expr("percentile(duration_secs, 0.5)") / 60, 1).alias("median_min"),
        F.round(F.expr("percentile(duration_secs, 0.95)") / 60, 1).alias("p95_min")
    )

# 4. top stations for circular trips (by circular count)
top_circ_stations = df.filter(F.col("is_circular")) \
    .groupBy("start_station_name") \
    .agg(F.count("*").alias("circular_rides")) \
    .orderBy(F.desc("circular_rides")) \
    .limit(20)

# 5. stations with highest circular RATE (min 500 total rides)
station_totals = df.groupBy("start_station_name") \
    .agg(
        F.count("*").alias("total_rides"),
        F.sum(F.when(F.col("is_circular"), 1).otherwise(0)).alias("circular_rides")
    ) \
    .filter(F.col("total_rides") > 500) \
    .withColumn("pct_circular", F.round(F.col("circular_rides") / F.col("total_rides") * 100, 1)) \
    .orderBy(F.desc("pct_circular")) \
    .limit(20)

user_rows = circ_by_user.collect()
dur_rows = dur_comparison.collect()
top_count_rows = top_circ_stations.collect()
top_rate_rows = station_totals.collect()

import json
import os

output = {
    "overall": {
        "total_rides": total,
        "circular_rides": circ_total,
        "pct_circular": pct_circ
    },
    "by_user_type": {},
    "duration_comparison": {},
    "top_by_count": [],
    "top_by_rate": []
}

for row in user_rows:
    output["by_user_type"][row.member_casual] = {
        "total_rides": int(row.total),
        "circular_rides": int(row.circular),
        "pct_circular": float(row.pct_circular)
    }

for row in dur_rows:
    label = "circular" if row.is_circular else "one_way"
    output["duration_comparison"][label] = {
        "rides": int(row.rides),
        "avg_min": float(row.avg_min),
        "median_min": float(row.median_min),
        "p95_min": float(row.p95_min)
    }

for row in top_count_rows:
    output["top_by_count"].append({
        "station": row.start_station_name,
        "circular_rides": int(row.circular_rides)
    })

for row in top_rate_rows:
    output["top_by_rate"].append({
        "station": row.start_station_name,
        "total_rides": int(row.total_rides),
        "circular_rides": int(row.circular_rides),
        "pct_circular": float(row.pct_circular)
    })

os.makedirs("/app/output", exist_ok=True)
with open("/app/output/circular_trips.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"Wrote circular_trips.json")
# top_rate_rows.show()

spark.stop()

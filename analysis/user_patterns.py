from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("user-patterns") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
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

# derived columns
df = df.withColumn("hour", F.hour("started_at_ts"))
# convert Spark dayofweek (1=Sunday) to European (1=Monday)
df = df.withColumn("dow_raw", F.dayofweek("started_at_ts"))
df = df.withColumn("dow",
    F.when(F.col("dow_raw") == 1, 7).otherwise(F.col("dow_raw") - 1))
df = df.withColumn("is_weekend",
    F.when(F.col("dow").isin([6, 7]), "weekend").otherwise("weekday"))

# filter bad durations (negative or 0 or unreasonably long)
df = df.filter((F.col("duration_secs") > 0) & (F.col("duration_secs") < 86400))

# 1. rides by hour per user type
hourly = df.groupBy("member_casual", "hour") \
    .agg(
        F.count("*").alias("rides"),
        F.round(F.avg("duration_secs") / 60, 1).alias("avg_duration_min")
    ) \
    .orderBy("member_casual", "hour")

# 2. rides by day of week per user type
daily = df.groupBy("member_casual", "dow") \
    .agg(F.count("*").alias("rides")) \
    .orderBy("member_casual", "dow")

# 3. weekday vs weekend per user type
weekend_stats = df.groupBy("member_casual", "is_weekend") \
    .agg(
        F.count("*").alias("rides"),
        F.round(F.avg("duration_secs") / 60, 1).alias("avg_duration_min")
    ) \
    .orderBy("member_casual", "is_weekend")

# 4. overall stats per user type
overall = df.groupBy("member_casual") \
    .agg(
        F.count("*").alias("total_rides"),
        F.round(F.avg("duration_secs") / 60, 1).alias("avg_duration_min"),
        F.round(F.expr("percentile(duration_secs, 0.5)") / 60, 1).alias("median_duration_min")
    )

hourly_rows = hourly.collect()
daily_rows = daily.collect()
weekend_rows = weekend_stats.collect()
overall_rows = overall.collect()

import json
import os

day_names = ["", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

output = {
    "overall": {},
    "by_hour": {"member": [], "casual": []},
    "by_day": {"member": [], "casual": []},
    "by_weekend": {"member": {}, "casual": {}}
}

for row in overall_rows:
    output["overall"][row.member_casual] = {
        "total_rides": int(row.total_rides),
        "avg_duration_min": float(row.avg_duration_min),
        "median_duration_min": float(row.median_duration_min)
    }

for row in hourly_rows:
    entry = {"hour": int(row.hour), "rides": int(row.rides), "avg_duration_min": float(row.avg_duration_min)}
    output["by_hour"][row.member_casual].append(entry)

for row in daily_rows:
    output["by_day"][row.member_casual].append({
        "day": day_names[int(row.dow)],
        "rides": int(row.rides)
    })

for row in weekend_rows:
    output["by_weekend"][row.member_casual][row.is_weekend] = {
        "rides": int(row.rides),
        "avg_duration_min": float(row.avg_duration_min)
    }

os.makedirs("/app/output", exist_ok=True)
with open("/app/output/user_patterns.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"Wrote user_patterns.json")

spark.stop()

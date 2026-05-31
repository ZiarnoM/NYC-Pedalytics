from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("seasonal-impact") \
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

df = df.withColumn("month", F.month("started_at_ts"))
df = df.withColumn("year", F.year("started_at_ts"))

# season
df = df.withColumn("season",
    F.when(F.col("month").isin([12, 1, 2]), "winter")
     .when(F.col("month").isin([3, 4, 5]), "spring")
     .when(F.col("month").isin([6, 7, 8]), "summer")
     .otherwise("fall"))

# 1. ride volume by season
volume = df.groupBy("season") \
    .agg(F.count("*").alias("rides")) \
    .orderBy("season")

# 2. duration by season (median + p95)
dur_by_season = df.groupBy("season") \
    .agg(
        F.round(F.avg("duration_secs") / 60, 1).alias("avg_min"),
        F.round(F.expr("percentile(duration_secs, 0.5)") / 60, 1).alias("median_min"),
        F.round(F.expr("percentile(duration_secs, 0.95)") / 60, 1).alias("p95_min")
    ) \
    .orderBy("season")

# 3. bike type mix by season
bike_mix = df.groupBy("season", "rideable_type") \
    .agg(F.count("*").alias("rides")) \
    .orderBy("season", "rideable_type")

# 4. user type mix by season
user_mix = df.groupBy("season", "member_casual") \
    .agg(F.count("*").alias("rides")) \
    .orderBy("season", "member_casual")

# 5. year-over-year by season (to see if growth differs by season)
yoy_season = df.groupBy("year", "season") \
    .agg(F.count("*").alias("rides")) \
    .orderBy("year", "season")

# collect
vol_rows = volume.collect()
dur_rows = dur_by_season.collect()
bike_rows = bike_mix.collect()
user_rows = user_mix.collect()
yoy_rows = yoy_season.collect()

import json
import os

output = {
    "volume": {},
    "duration": {},
    "bike_mix": {},
    "user_mix": {},
    "yoy_by_season": {}
}

for row in vol_rows:
    output["volume"][row.season] = int(row.rides)

for row in dur_rows:
    output["duration"][row.season] = {
        "avg_min": float(row.avg_min),
        "median_min": float(row.median_min),
        "p95_min": float(row.p95_min)
    }

for row in bike_rows:
    if row.season not in output["bike_mix"]:
        output["bike_mix"][row.season] = {}
    output["bike_mix"][row.season][row.rideable_type] = int(row.rides)

for row in user_rows:
    if row.season not in output["user_mix"]:
        output["user_mix"][row.season] = {}
    output["user_mix"][row.season][row.member_casual] = int(row.rides)

for row in yoy_rows:
    yr = str(int(row.year))
    if yr not in output["yoy_by_season"]:
        output["yoy_by_season"][yr] = {}
    output["yoy_by_season"][yr][row.season] = int(row.rides)

# add e-bike % per season
for season in output["bike_mix"]:
    classic = output["bike_mix"][season].get("classic_bike", 0)
    electric = output["bike_mix"][season].get("electric_bike", 0)
    total = classic + electric
    output["bike_mix"][season]["pct_electric"] = round(electric / total * 100, 1) if total > 0 else 0

# add casual % per season
for season in output["user_mix"]:
    member = output["user_mix"][season].get("member", 0)
    casual = output["user_mix"][season].get("casual", 0)
    total = member + casual
    output["user_mix"][season]["pct_casual"] = round(casual / total * 100, 1) if total > 0 else 0

os.makedirs("/app/output", exist_ok=True)
with open("/app/output/seasonal_impact.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"Wrote seasonal_impact.json")
# vol_rows.show()

spark.stop()

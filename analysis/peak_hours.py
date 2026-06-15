from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("peak-hours") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/data/*.csv")

print(f"Loaded {df.count()} rows")

# parse timestamps
df = df.withColumn("started_at_ts", F.to_timestamp("started_at"))
df = df.filter(F.col("started_at_ts").isNotNull())

# derived columns
df = df.withColumn("hour", F.hour("started_at_ts"))
df = df.withColumn("month", F.month("started_at_ts"))
df = df.withColumn("dow", F.dayofweek("started_at_ts"))

# weekend flag
df = df.withColumn("is_weekend",
    F.when(F.col("dow").isin([1, 7]), "weekend").otherwise("weekday"))

# season
df = df.withColumn("season",
    F.when(F.col("month").isin([12, 1, 2]), "winter")
     .when(F.col("month").isin([3, 4, 5]), "spring")
     .when(F.col("month").isin([6, 7, 8]), "summer")
     .otherwise("fall"))

# group by hour, weekend/weekday, season
peak = df.groupBy("hour", "is_weekend", "season") \
    .agg(F.count("*").alias("rides")) \
    .orderBy("is_weekend", "season", "hour")

# also compute by hour, weekend/weekday without season
peak_simple = df.groupBy("hour", "is_weekend") \
    .agg(F.count("*").alias("rides")) \
    .orderBy("is_weekend", "hour")

full_rows = peak.collect()
simple_rows = peak_simple.collect()

import json
import os

output = {
    "by_season": {"weekday": {}, "weekend": {}},
    "simple": {"weekday": [], "weekend": []}
}

for row in full_rows:
    wkey = row.is_weekend
    skey = row.season
    if skey not in output["by_season"][wkey]:
        output["by_season"][wkey][skey] = []
    output["by_season"][wkey][skey].append({
        "hour": int(row.hour),
        "rides": int(row.rides)
    })

for row in simple_rows:
    output["simple"][row.is_weekend].append({
        "hour": int(row.hour),
        "rides": int(row.rides)
    })

os.makedirs("/app/output", exist_ok=True)
with open("/app/output/peak_hours.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"Wrote peak_hours.json")

spark.stop()

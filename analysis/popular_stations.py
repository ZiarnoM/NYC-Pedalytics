from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("popular-stations") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/data/*.csv")

print(f"Loaded {df.count()} rows")
# df.printSchema()

# parse timestamps and compute derived columns
df = df.withColumn("started_at_ts", F.to_timestamp("started_at"))
df = df.withColumn("ended_at_ts", F.to_timestamp("ended_at"))
df = df.withColumn("month", F.month("started_at_ts"))
df = df.withColumn("year", F.year("started_at_ts"))

# drop rows with bad timestamps
df = df.filter(F.col("started_at_ts").isNotNull() & F.col("ended_at_ts").isNotNull())

# count starts per station per month
starts = df.groupBy("start_station_name", "year", "month") \
    .agg(F.count("*").alias("start_count"))

# count ends per station per month
ends = df.groupBy("end_station_name", "year", "month") \
    .agg(F.count("*").alias("end_count"))

# join starts and ends to get asymmetry
station_stats = starts.join(
    ends,
    (starts.start_station_name == ends.end_station_name) &
    (starts.year == ends.year) &
    (starts.month == ends.month),
    "outer"
).fillna(0)

station_stats = station_stats.withColumn(
    "station",
    F.when(F.col("start_station_name") != 0, F.col("start_station_name"))
     .otherwise(F.col("end_station_name"))
).withColumn(
    "yr", F.coalesce(starts.year, ends.year).cast("int")
).withColumn(
    "mo", F.coalesce(starts.month, ends.month).cast("int")
).withColumn(
    "net", F.col("end_count") - F.col("start_count")
).withColumn(
    "total_activity", F.col("start_count") + F.col("end_count")
).select("station", "yr", "mo", "start_count", "end_count", "net", "total_activity")

# top 20 stations per month by start_count
from pyspark.sql.window import Window

monthly_window = Window.partitionBy("yr", "mo").orderBy(F.desc("start_count"))
top_monthly = station_stats.withColumn("rank", F.row_number().over(monthly_window)) \
    .filter(F.col("rank") <= 20) \
    .drop("rank")

# also compute all-time top stations (across both years)
alltime = station_stats.groupBy("station") \
    .agg(
        F.sum("start_count").alias("total_starts"),
        F.sum("end_count").alias("total_ends"),
        F.sum("net").alias("total_net")
    ) \
    .orderBy(F.desc("total_starts")) \
    .limit(30)

monthly_results = top_monthly.orderBy("yr", "mo", F.desc("start_count")).collect()
alltime_results = alltime.collect()

import json
import os

output = {
    "by_month": {},
    "alltime_top30": []
}

for row in monthly_results:
    key = f"{row.yr}-{row.mo:02d}"
    if key not in output["by_month"]:
        output["by_month"][key] = []
    output["by_month"][key].append({
        "station": row.station,
        "start_count": int(row.start_count),
        "end_count": int(row.end_count),
        "net": int(row.net),
        "total_activity": int(row.total_activity)
    })

for row in alltime_results:
    output["alltime_top30"].append({
        "station": row.station,
        "total_starts": int(row.total_starts),
        "total_ends": int(row.total_ends),
        "total_net": int(row.total_net)
    })

os.makedirs("/app/output", exist_ok=True)
with open("/app/output/popular_stations.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"Wrote popular_stations.json — {len(output['by_month'])} months, {len(output['alltime_top30'])} all-time stations")

spark.stop()

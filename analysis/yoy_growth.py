from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("yoy-growth") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/data/*.csv")

print(f"Loaded {df.count()} rows")

# parse timestamps
df = df.withColumn("started_at_ts", F.to_timestamp("started_at"))
df = df.filter(F.col("started_at_ts").isNotNull())

# extract year and month
df = df.withColumn("year", F.year("started_at_ts"))
df = df.withColumn("month", F.month("started_at_ts"))

# count rides per year and month
monthly = df.groupBy("year", "month") \
    .agg(F.count("*").alias("rides")) \
    .orderBy("year", "month")

# also count by user type for a more detailed view
monthly_by_type = df.groupBy("year", "month", "member_casual") \
    .agg(F.count("*").alias("rides")) \
    .orderBy("year", "month", "member_casual")

rows = monthly.collect()
type_rows = monthly_by_type.collect()

import json
import os

# build lookup: (year, month) -> rides
lookup = {}
for row in rows:
    lookup[(int(row.year), int(row.month))] = int(row.rides)

month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

output = {
    "monthly": [],
    "yearly_totals": {}
}

total_2023 = 0
total_2024 = 0

for m in range(1, 13):
    rides_2023 = lookup.get((2023, m), 0)
    rides_2024 = lookup.get((2024, m), 0)
    total_2023 += rides_2023
    total_2024 += rides_2024

    if rides_2023 > 0:
        change_pct = round((rides_2024 - rides_2023) / rides_2023 * 100, 1)
    else:
        change_pct = None

    output["monthly"].append({
        "month": m,
        "month_name": month_names[m],
        "rides_2023": rides_2023,
        "rides_2024": rides_2024,
        "change_pct": change_pct
    })

output["yearly_totals"] = {
    "2023": total_2023,
    "2024": total_2024,
    "growth_pct": round((total_2024 - total_2023) / total_2023 * 100, 1) if total_2023 > 0 else None
}

# add user type breakdown by month
by_type_data = {}
for row in type_rows:
    y = int(row.year)
    m = int(row.month)
    key = f"{y}-{m:02d}"
    if key not in by_type_data:
        by_type_data[key] = {}
    by_type_data[key][row.member_casual] = int(row.rides)

output["by_user_type"] = by_type_data

os.makedirs("/app/output", exist_ok=True)
with open("/app/output/yoy_growth.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"Wrote yoy_growth.json")
print(f"2023 total: {total_2023:,} | 2024 total: {total_2024:,} | growth: {output['yearly_totals']['growth_pct']}%")

spark.stop()

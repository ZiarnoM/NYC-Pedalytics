# NYC-Pedalytics

Analyzing Citi Bike NYC trip data (2023-2024) using Apache Hadoop and Spark. The project looks at how people use the bike-sharing system — popular stations, peak hours, bike type trends, and differences between members and casual riders. It also includes a simple recommendation feature.

## Dataset

The [Citi Bike System Data](https://citibikenyc.com/system-data) is published monthly. This project uses 2023 and 2024 — about 30-50 GB of CSV data spread across 13 files. Each row is a single trip with start/end station names, coordinates, timestamps, bike type, and rider type.

## Tech stack

- Apache Hadoop (HDFS) for storage
- Apache Spark (PySpark) for analysis
- Single-node Docker Compose cluster
- CLI app for browsing results

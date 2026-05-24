# NYC-Pedalytics

Analyzing Citi Bike NYC trip data (2023-2024) using Apache Hadoop and Spark. The project looks at how people use the bike-sharing system — popular stations, peak hours, bike type trends, and differences between members and casual riders. It also includes a simple recommendation feature.

## Dataset

The [Citi Bike System Data](https://citibikenyc.com/system-data) is published monthly. This project uses 2023 and 2024 — about 30-50 GB of CSV data spread across 13 files. Each row is a single trip with start/end station names, coordinates, timestamps, bike type, and rider type.

## Tech stack

- Apache Hadoop (HDFS) for storage
- Apache Spark (PySpark) for analysis
- Single-node Docker Compose cluster
- CLI app for browsing results

## How to run

### 1. Start the cluster

```bash
cd docker
docker compose up -d
```

This starts HDFS (namenode + datanode) and Spark (master + worker). Their web UIs are at:

- Namenode: http://localhost:9870
- Spark Master: http://localhost:8080

### 2. Download the data

```bash
bash scripts/download_data.sh
```

Fetches 13 zip files (~10 GB) from Citi Bike's S3 bucket into `data/`. This can take a while depending on your connection.

### 3. Extract CSVs

```bash
python3 scripts/extract.py
```

Unzips all downloaded files, skips Jersey City records, writes CSVs to `csv/`. Expect ~15 GB of CSV data.

### 4. Load to HDFS

```bash
python3 scripts/load_to_hdfs.py
```

Copies all CSVs from `csv/` into HDFS under `/data/`. The cluster must be running (step 1).

### 5. Run analysis and browse results

...

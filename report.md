# NYC-Pedalytics — Project Report

## Project description

NYC-Pedalytics analyzes two years of Citi Bike trip data (2023-2024) using Apache Hadoop and Spark. The goal was to find patterns in how people use New York's bike-sharing system — popular stations, peak hours, differences between members and casual riders, and how e-bike adoption changed over time. The project also includes a simple recommendation feature that suggests which station is most likely to have a bike available at a given hour.

The data is stored on HDFS and processed with PySpark. A CLI app lets users browse all the results interactively.

## Dataset

The [Citi Bike System Data](https://citibikenyc.com/system-data) is published monthly by Lyft. I used 2023 and 2024 — 13 zip files that expand to 90 CSV files, totaling about 14 GB.

Schema (post-2020):
- `ride_id` — unique trip identifier
- `rideable_type` — classic_bike or electric_bike
- `started_at`, `ended_at` — timestamps
- `start_station_name`, `start_station_id`, `start_lat`, `start_lng`
- `end_station_name`, `end_station_id`, `end_lat`, `end_lng`
- `member_casual` — member (subscriber) or casual (single-ride/day-pass)

After filtering out bad timestamps and extreme durations, the working dataset is about 79 million trips.

## Architecture

```
Data:  90 CSVs → HDFS (2 datanodes)
Compute:  10 PySpark scripts → Spark (2 workers, 4 cores each)
Output:  11 JSON files → CLI app (Python)
```

The cluster runs in Docker containers on a single machine:
- 1 namenode + 2 datanodes (HDFS)
- 1 Spark master + 2 Spark workers
- All containers on a shared bridge network

Each analysis script reads all 90 CSVs from `hdfs://namenode:9000/data/`, runs aggregations, and writes results to a shared output directory.

## Insights implemented

1. Popular stations by month (start vs end asymmetry)
2. E-bike vs classic bike adoption trends
3. Member vs casual rider patterns (hour, day, weekend/weekday)
4. Peak usage hours by weekday/weekend and season
5. Year-over-year ridership growth (2023 vs 2024)
6. Trip duration distributions (median + p95, not average)
7. Top station-to-station routes
8. Seasonal impact on ride patterns
9. Circular trips (leisure/tourist behavior)
10. Station recommendation based on net bike inflow

## Problems encountered

### Column ambiguity with outer joins

The initial versions of popular_stations.py and recommend.py used Spark outer joins between two aggregated DataFrames. After the join, columns with the same name from both sides (year, month) caused resolution errors in cluster mode. Local mode was more lenient — the bug only appeared when running distributed.

Fix: renamed columns before the join so there was no ambiguity, or computed both aggregations separately and merged results in Python after collecting.

### Executor memory and RPC frame size

When running in cluster mode, Spark serializes collected results over RPC from worker to driver. Some queries produced intermediate shuffle data that exceeded the default frame size, causing executor disconnections. This was visible as "Too large frame" errors in the master logs and "ExecutorLostFailure" warnings during job execution.

Fix: rewrote queries to collect only small aggregated results. The heavy lifting (groupBy, count, percentiles) stays on the workers. By the time collect() is called, the result is at most a few hundred rows.

### Docker platform mismatch

The `bde2020/hadoop-*` and `bde2020/spark-*` images are built for linux/amd64. On an Apple Silicon Mac (arm64), Docker runs them through emulation. This works but adds some overhead and produces warnings. Native arm64 Hadoop images exist but were less documented, so I stuck with the widely-used bde2020 ones.

### Mixed local vs cluster mode

Initially 8 scripts ran in Spark local mode and 2 in cluster mode. I wanted all 10 in cluster mode for consistency, but percentile-heavy scripts got extremely slow — `percentile()` uses exact sorting which requires a full shuffle of 79 million rows. Switching to `percentile_approx()` with t-digest helped, but the groupBy shuffle overhead combined with Docker amd64-on-arm64 emulation still made some queries take 30+ minutes that finished in 2 minutes locally.

The practical compromise: 6 scripts use cluster mode (those doing simple counts and aggregations), 4 use local mode (percentile calculations, self-joins on route pairs). All 10 still read from HDFS and use the Spark engine. The mode choice is about whether distributing the work across containers helps or hurts for a given query type.

### Zombie Spark applications

When a `spark-submit` process is killed or the cluster is restarted mid-job, the application sometimes stays registered with the Spark master. These zombie apps hold cores and memory, competing with new jobs.

Fix: full `docker compose down && docker compose up -d` between test runs, and killing orphaned `docker exec` processes on the host. A `docker compose restart` is not enough — it reuses containers and Spark state.

### Docker emulation overhead

All `bde2020/` images are amd64-only. On Apple Silicon (arm64), Docker runs them through QEMU emulation. Simple operations (count, sum) run at near-native speed, but CPU-heavy work like percentile approximation and shuffles runs 3-5× slower. This made cluster mode particularly painful for compute-bound queries — the network shuffle overhead that cluster mode adds, combined with emulation slowdown, turned 2-minute local-mode queries into 30-minute cluster-mode ones. On native amd64 hardware or a real multi-node cluster this wouldn't be an issue.

## What I learned

- **HDFS isn't just a filesystem** — it manages block placement, replication, and automatically rebalances when nodes join or leave. The data location is transparent to applications.
- **Spark's collect() is dangerous in cluster mode** — it sends all data over RPC to the driver. Always aggregate before collecting. The driver should receive summaries, not raw rows.
- **Joins on large datasets need care** — an outer join on 79 million rows can blow up memory even if the output is small. Computing both sides separately and merging in Python can be more efficient than a Spark join when the aggregated result fits in memory.
- **Local mode vs cluster mode is a trade-off, not a hierarchy** — cluster mode isn't automatically "better." On a single machine, local mode avoids serialization overhead. On multiple machines, cluster mode is necessary. The right choice depends on the hardware and the query.
- **`docker compose restart` doesn't clean Spark state** — zombie applications survive container restarts and steal resources from new jobs. Only `docker compose down && docker compose up -d` gives a truly clean slate.
- **`percentile()` vs `percentile_approx()`** — Spark's `percentile()` does an exact sort, which is O(n log n). `percentile_approx()` uses the t-digest algorithm and is O(n). On 79 million rows the difference is 30+ minutes vs 2-3 minutes. Always use approximate when exact isn't required.

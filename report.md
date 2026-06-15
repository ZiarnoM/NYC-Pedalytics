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
Data:  90 CSVs → HDFS (3 datanodes, replication factor 2)
Compute:  10 PySpark scripts → Spark (2 workers, 4 cores each)
Output:  11 JSON files → CLI app (Python)
```

The cluster runs in Docker containers on a single machine:
- 1 namenode + 3 datanodes (HDFS, RF=2 so blocks spread across 2 of 3)
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

### Executor memory and OOM kills (the biggest problem)

The default executor memory in bde2020 Spark images is 1024 MB. Simple aggregations with few keys (like year/month — 24 keys) fit in this, but station-level aggregations (~2000 stations × 24 hours = 48,000 keys) caused massive shuffle spilling to disk. On QEMU-emulated disk, shuffle reads timed out, executors were killed, and stages kept retrying. The popular_stations.py job ran for 48 minutes with 14 stage attempts before succeeding.

First attempt: use `--executor-memory 3g` flag. This was **ignored** — the bde2020 Spark image in standalone mode doesn't respect the `--executor-memory` CLI flag. Executors still got 1024 MB.

Second attempt: set `spark.executor.memory` in SparkSession config. This worked for lighter shuffles but station-level shuffles still killed executors. The problem: executor JVM memory (2 GB) + worker JVM overhead exceeded the worker container's 4 GB limit. Even at 3 GB executor + 1 GB worker = exactly 4 GB, JVM metaspace and native memory pushed it over, triggering the OOM killer.

Final fix for recommend.py: run in local mode (no executor containers) and only read the 4 needed columns instead of all columns. For the other cluster-mode scripts, executor memory was set to 2 GB in SparkSession config, which leaves enough headroom in the worker container for simpler shuffles.

Key lesson: on bde2020 Docker images, always set executor memory via `spark.executor.memory` in the SparkSession builder, not via `--executor-memory` CLI flag. And be aware that the worker container's total memory must fit both the worker JVM and all executor JVMs running inside it.

### Executor memory flag silently ignored

bde2020 Spark images in standalone mode ignore the `--executor-memory` flag passed to `spark-submit`. Executors always get the default 1024 MB regardless of what the flag says. This cost several hours of debugging — the flag appears in every tutorial and example, but silently does nothing on this specific image.

Fix: set `spark.executor.memory` via `.config()` in the SparkSession builder inside the Python script. This works.

### Zombie Spark applications

When a `spark-submit` process is killed or the cluster is restarted mid-job, the application sometimes stays registered with the Spark master. These zombie apps hold cores and memory, competing with new jobs.

Fix: full `docker compose down && docker compose up -d` between test runs, and killing orphaned `docker exec` processes on the host. A `docker compose restart` is not enough — it reuses containers and Spark state.

### Worker daemon + executor memory collision (the hardest problem)

The `SPARK_WORKER_MEMORY` env var in bde2020 images controls the worker daemon JVM's heap size via `-Xmx`. I initially set this to 4 GB, thinking more memory is always better. But the Spark worker daemon and the executor JVM run in the **same Docker container**. With `SPARK_WORKER_MEMORY=4g` (daemon `-Xmx4g`) and executor `-Xmx2g`, the combined virtual memory reservation was ~6 GB per worker container. Docker Desktop's default VM memory (~4-6 GB shared across all 7 containers) couldn't satisfy this — the Linux OOM killer terminated the executor with code 137.

The result: `recommend.py` failed repeatedly. Stages retried 13+ times over 48 minutes because executors kept dying mid-shuffle. The error message — "Remote RPC client disassociated... Command exited with code 137" — pointed to a network issue, but the real cause was memory. This cost several hours to diagnose because every guide and tutorial tells you to increase `SPARK_WORKER_MEMORY`, not decrease it.

The fix was counterintuitive: **reduce** `SPARK_WORKER_MEMORY` from 4 GB to 2 GB, and set executor memory to 1.5 GB. Combined with increasing Docker Desktop's VM memory to 10 GB, this gave containers enough headroom. After the fix, `recommend.py` completed in 2 minutes with zero executor failures.

Lesson: in containerized Spark, the worker daemon and executors compete for memory inside the same container. `SPARK_WORKER_MEMORY` must leave room for the executor. With bde2020 images on a single Docker host, 2 GB worker + 1.5 GB executor per container is the safe configuration.

## What I learned

- **HDFS isn't just a filesystem** — it manages block placement, replication, and automatically distributes blocks when nodes join. With 3 datanodes and RF=2, no single node holds the full dataset (~110 blocks each, not all ~167).
- **bde2020 Spark ignores `--executor-memory`** — the flag is silently ignored in standalone mode. Set `spark.executor.memory` in SparkSession config instead.
- **Executor OOM kills are a container limit problem** — even with correct Spark config, the worker container's total memory must fit worker JVM + executor JVMs. Code 137 = Linux OOM killer, not a Spark error.
- **Don't cache blindly** — caching 79 million rows on a 6 GB cluster causes more spilling than it saves. The cache itself becomes a bottleneck.
- **Spark's collect() is dangerous in cluster mode** — it sends all data over RPC to the driver. Always aggregate before collecting. The driver should receive summaries, not raw rows.
- **Joins on large datasets need care** — an outer join on 79 million rows can blow up memory even if the output is small. Computing both sides separately and merging in Python can be more efficient than a Spark join when the aggregated result fits in memory.
- **Local mode vs cluster mode is a trade-off, not a hierarchy** — cluster mode isn't automatically "better." On a single machine, local mode avoids serialization, network, and container memory overhead. The right choice depends on the hardware and the query.
- **`docker compose restart` doesn't clean Spark state** — zombie applications survive container restarts and steal resources from new jobs. Only `docker compose down && docker compose up -d` gives a truly clean slate.
- **`percentile()` vs `percentile_approx()`** — Spark's `percentile()` does an exact sort, which is O(n log n). `percentile_approx()` uses the t-digest algorithm and is O(n). On 79 million rows the difference is 30+ minutes vs 2-3 minutes.
- **Replication factor matters for demonstrating distribution** — with 2 nodes, RF degrades to full mirroring. You need at least n+1 nodes (e.g. 3 nodes with RF=2) to show that no single node holds everything.

import subprocess
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
CSV_DIR = os.path.join(PROJECT_DIR, "csv")

NAMENODE = "namenode"
HDFS_DIR = "/data"


def run(cmd):
    print(f"  $ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.strip())
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr.strip()}")
        sys.exit(1)
    return result.stdout.strip()


# check that namenode container is running
try:
    out = subprocess.run(
        f"docker ps --filter name={NAMENODE} --format '{{{{.Names}}}}'",
        shell=True,
        capture_output=True,
        text=True,
    )
    running = out.stdout.strip()
    if NAMENODE not in running:
        print(f"ERROR: Container '{NAMENODE}' is not running.")
        sys.exit(1)
    print(f"  OK - {NAMENODE} is running\n")
except Exception as e:
    print(f"ERROR: Could not check Docker containers: {e}")
    sys.exit(1)

# check HDFS is reachable
print("Checking HDFS connectivity...")
run(f"docker exec {NAMENODE} hdfs dfs -ls / 2>&1")

# create the target dir in HDFS
print(f"\nCreating HDFS directory {HDFS_DIR}...")
run(f"docker exec {NAMENODE} hdfs dfs -mkdir -p {HDFS_DIR}")

# list csv files to upload
csv_files = sorted([f for f in os.listdir(CSV_DIR) if f.endswith(".csv")])
print(f"\nFound {len(csv_files)} CSV files to upload.")

# upload each file
uploaded = 0
for i, fname in enumerate(csv_files, 1):
    hdfs_path = f"{HDFS_DIR}/{fname}"

    # check if already uploaded
    check = subprocess.run(
        f"docker exec {NAMENODE} hdfs dfs -test -e {hdfs_path}",
        shell=True,
        capture_output=True,
    )
    if check.returncode == 0:
        print(f"  [{i}/{len(csv_files)}] {fname} - already exists, skipping")
        continue

    local_path = f"/mnt/csv/{fname}"
    print(f"  [{i}/{len(csv_files)}] {fname} ...")
    run(f"docker exec {NAMENODE} hdfs dfs -put {local_path} {hdfs_path}")
    uploaded += 1

print(f"\nDone. Uploaded {uploaded} new files to HDFS {HDFS_DIR}/")
print(f"Skipped {len(csv_files) - uploaded} files that already existed.")

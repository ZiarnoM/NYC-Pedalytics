#!/bin/bash

# Download Citi Bike NYC trip data (2023 + 2024)
# 2023: yearly bundle (~1.6 GB)
# 2024: 12 monthly files (~8.7 GB total)
# Total: ~10.3 GB compressed

BASE_URL="https://s3.amazonaws.com/tripdata"

# Directory where this script lives
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
mkdir -p "$DATA_DIR"

echo "=== Downloading Citi Bike NYC Trip Data ==="
echo "Saving to: ${DATA_DIR}"
echo ""

# 2023 yearly bundle
echo "[1/13] 2023 yearly bundle (~1.6 GB)..."
curl -o "${DATA_DIR}/2023-citibike-tripdata.zip" \
     "${BASE_URL}/2023-citibike-tripdata.zip"

# 2024 monthly files
for MONTH in 01 02 03 04 05 06 07 08 09 10 11 12; do
    FILE="2024${MONTH}-citibike-tripdata.zip"
    # +2 because 2023 was #1
    NUM=$((10#$MONTH + 1))
    echo "[${NUM}/13] ${FILE}..."
    curl -o "${DATA_DIR}/${FILE}" \
         "${BASE_URL}/${FILE}"
done

echo ""
echo "=== Done ==="
echo "Files downloaded to ${DATA_DIR}:"
ls -lh "${DATA_DIR}"

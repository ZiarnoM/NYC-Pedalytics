import zipfile
import os
import glob

# extract zips from data/ into csv/, skip Jersey City files
base_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(base_dir)
data_dir = os.path.join(project_dir, "data")
csv_dir = os.path.join(project_dir, "csv")

os.makedirs(csv_dir, exist_ok=True)

zips = sorted(glob.glob(os.path.join(data_dir, "*.zip")))
print(f"Found {len(zips)} zip files in {data_dir}")
print(f"Extracting to: {csv_dir}\n")

total_csvs = 0
total_skipped = 0

for zip_path in zips:
    zip_name = os.path.basename(zip_path)
    print(f"Processing {zip_name}...")

    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.namelist():
            fname = os.path.basename(member)

            # skip directories and non-CSV files
            if not fname.endswith(".csv"):
                continue

            # skip Jersey City files
            if fname.startswith("JC-"):
                total_skipped += 1
                continue

            # skip __MACOSX junk
            if member.startswith("__MACOSX"):
                continue

            target = os.path.join(csv_dir, fname)

            # if file already exists, skip (avoid dupes from overlapping datasets)
            if os.path.exists(target):
                continue

            with zf.open(member) as src, open(target, "wb") as dst:
                dst.write(src.read())

            total_csvs += 1

print(f"\nDone. Extracted {total_csvs} CSV files, skipped {total_skipped} JC files.")

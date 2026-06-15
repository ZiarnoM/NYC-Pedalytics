import zipfile
import os
import glob
import io

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


def should_skip(fname):
    """Return True if this file should be skipped."""
    if not fname.endswith(".csv"):
        return True
    if fname.startswith("JC-"):
        return True
    return False


def extractIcsv_from_zip(zf, member_name, dst_dir):
    """Extract a single CSV entry to dst_dir, return True if extracted."""
    global total_csvs, total_skipped

    fname = os.path.basename(member_name)

    if fname.startswith("JC-"):
        total_skipped += 1
        return False

    target = os.path.join(dst_dir, fname)
    if os.path.exists(target):
        return False

    with zf.open(member_name) as src, open(target, "wb") as dst:
        dst.write(src.read())

    total_csvs += 1
    return True


for zip_path in zips:
    zip_name = os.path.basename(zip_path)
    print(f"Processing {zip_name}...")

    with zipfile.ZipFile(zip_path, "r") as outer_zf:
        for entry in outer_zf.namelist():
            entry_name = os.path.basename(entry)

            # skip dirs, hidden files, mac junk
            if entry.startswith("__MACOSX") or entry_name.startswith("._"):
                continue
            if entry.endswith("/") or entry_name == ".DS_Store":
                continue

            # direct CSV
            if entry_name.endswith(".csv"):
                extract_csv_from_zip(outer_zf, entry, csv_dir)

            # nested zip (eg. 2023 yearly contains monthly zips)
            elif entry_name.endswith(".zip"):
                print(f"  -> nested: {entry_name}")
                with outer_zf.open(entry) as nested_data:
                    nested_bytes = io.BytesIO(nested_data.read())
                    with zipfile.ZipFile(nested_bytes, "r") as inner_zf:
                        for inner_entry in inner_zf.namelist():
                            inner_name = os.path.basename(inner_entry)
                            if inner_entry.startswith(
                                "__MACOSX"
                            ) or inner_name.startswith("._"):
                                continue
                            if inner_entry.endswith("/") or inner_name == ".DS_Store":
                                continue
                            if inner_name.endswith(".csv"):
                                extract_csv_from_zip(inner_zf, inner_entry, csv_dir)

print(f"\nDone. Extracted {total_csvs} CSV files, skipped {total_skipped} JC files.")

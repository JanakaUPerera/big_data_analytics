#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy
# pip install tqdm

import time
from pathlib import Path

import pandas as pd
from sodapy import Socrata
from tqdm.auto import tqdm

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("data.austintexas.gov", None, timeout=60)

# Example authenticated client (needed for non-public datasets):
# client = Socrata(data.austintexas.gov,
#                  MyAppToken,
#                  username="user@example.com",
#                  password="AFakePassword")

dataset_id = "sh59-i6y9"
data_dir = Path(__file__).resolve().parent.parent / "data"
data_dir.mkdir(parents=True, exist_ok=True)
page_size = 5000  # smaller pages are less likely to trigger a mid-stream reset
rows_per_file = 500_000  # split output into files of this many rows each
max_retries = 5
total_size = 82108605 # Total dataset size (Number of row count)

offset = 2_000_000
file_index = 5
rows_in_current_file = 0
first_page_in_file = True

with tqdm(total=total_size, unit="rows") as pbar:
    while True:
        for attempt in range(1, max_retries + 1):
            try:
                page = client.get(dataset_id, limit=page_size, offset=offset)
                break
            except Exception as e:
                wait = 2 ** attempt
                pbar.write(f"Error fetching offset {offset} (attempt {attempt}/{max_retries}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
        else:
            raise RuntimeError(f"Failed to fetch offset {offset} after {max_retries} attempts")

        if not page:
            break

        # Append each page straight to disk so a later failure doesn't lose
        # rows already downloaded.
        csv_path = data_dir / f"sh59-i6y9_part{file_index}.csv"
        page_df = pd.DataFrame.from_records(page)
        page_df.to_csv(csv_path, mode="w" if first_page_in_file else "a", header=first_page_in_file, index=False)
        first_page_in_file = False

        offset += len(page)
        rows_in_current_file += len(page)
        pbar.update(len(page))

        if rows_in_current_file >= rows_per_file:
            file_index += 1
            rows_in_current_file = 0
            first_page_in_file = True

        if len(page) < page_size:
            break

print("Download complete.")
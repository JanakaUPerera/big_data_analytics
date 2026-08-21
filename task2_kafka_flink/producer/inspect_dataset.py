from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data"

files = sorted(DATA_DIR.glob("sh59-i6y9_part*.csv"))

print(f"Files found: {len(files)}")

total_rows = 0

for file in files:
    df = pd.read_csv(file)

    print(f"\nFile: {file.name}")
    print(f"Rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    total_rows += len(df)

print(f"\nTotal rows across all files: {total_rows}")
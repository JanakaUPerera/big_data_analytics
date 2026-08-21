import json
from pathlib import Path

import pandas as pd


DATA_DIR = Path(__file__).resolve().parents[1] / "data"

files = sorted(DATA_DIR.glob("sh59-i6y9_part*.csv"))

if not files:
    raise FileNotFoundError("No dataset files found.")

# Read only the first record for testing
df = pd.read_csv(files[0], nrows=1)

row = df.iloc[0]

message = {
    "record_id": str(row["record_id"]),
    "atd_device_id": int(row["atd_device_id"]),
    "read_date": str(row["read_date"]),
    "intersection_name": str(row["intersection_name"]),
    "direction": str(row["direction"]),
    "movement": str(row["movement"]),
    "heavy_vehicle": bool(row["heavy_vehicle"]),
    "volume": int(row["volume"]),
    "speed_average": float(row["speed_average"]),
    "bin_duration": int(row["bin_duration"]),
}

print(json.dumps(message, indent=2))
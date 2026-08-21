import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer


DATA_DIR = Path(__file__).resolve().parents[1] / "data"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9192"
KAFKA_TOPIC = "traffic-telemetry"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


files = sorted(DATA_DIR.glob("sh59-i6y9_part*.csv"))

if not files:
    raise FileNotFoundError("No traffic CSV files found in the data directory.")


for file in files:
    print(f"\nReading: {file.name}")

    for chunk in pd.read_csv(file, chunksize=1000):

        for _, row in chunk.iterrows():

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

            producer.send(
                KAFKA_TOPIC,
                value=message,
            )

            producer.flush()

            print(
                f"Sent: device={message['atd_device_id']}, "
                f"time={message['read_date']}, "
                f"volume={message['volume']}"
            )

            time.sleep(2)
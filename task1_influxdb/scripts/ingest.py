import csv
import os
import math
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "coursework-token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "coventry-university")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "climate")

DATA_FILE = os.getenv(
    "DATA_FILE",
    "/data/alaska_airports_hourly_winds_PAFA.csv"
)

STATION = os.getenv("STATION", "PAFA")

BATCH_SIZE = 5000


def parse_float(value):
    """
    Convert a CSV value into float.
    Return None for missing or invalid values.
    """
    if value is None:
        return None

    value = value.strip()

    if value == "":
        return None

    try:
        number = float(value)

        if math.isnan(number):
            return None

        return number

    except ValueError:
        return None


def parse_timestamp(value):
    """
    Convert dataset timestamp into UTC datetime.
    """
    dt = datetime.strptime(
        value.strip(),
        "%Y-%m-%d %H:%M:%S"
    )

    return dt.replace(tzinfo=timezone.utc)


def create_point(row):

    timestamp = parse_timestamp(row["ts"])

    wind_speed = parse_float(row["ws"])
    wind_direction = parse_float(row["wd"])

    point = (
        Point("airport_wind")
        .tag("station", STATION)
        .time(timestamp, WritePrecision.S)
    )

    if wind_speed is not None:
        point.field("wind_speed", wind_speed)

    if wind_direction is not None:
        point.field("wind_direction", wind_direction)

    return point


def main():

    print("Starting InfluxDB ingestion")
    print(f"Dataset : {DATA_FILE}")
    print(f"Station : {STATION}")
    print(f"Bucket  : {INFLUX_BUCKET}")

    client = InfluxDBClient(
        url=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG
    )

    write_api = client.write_api(
        write_options=SYNCHRONOUS
    )

    batch = []

    total_rows = 0
    failed_rows = 0

    with open(DATA_FILE, "r", encoding="utf-8") as file:

        reader = csv.DictReader(file)

        for row in reader:

            try:
                point = create_point(row)

                batch.append(point)

                total_rows += 1

                if len(batch) >= BATCH_SIZE:

                    write_api.write(
                        bucket=INFLUX_BUCKET,
                        org=INFLUX_ORG,
                        record=batch
                    )

                    print(
                        f"Inserted {total_rows:,} records..."
                    )

                    batch.clear()

            except Exception as exc:

                failed_rows += 1

                print(
                    f"Failed row {total_rows + failed_rows}: "
                    f"{exc}"
                )

        if batch:

            write_api.write(
                bucket=INFLUX_BUCKET,
                org=INFLUX_ORG,
                record=batch
            )

    client.close()

    print()
    print("Ingestion completed.")
    print(f"Successful rows : {total_rows:,}")
    print(f"Failed rows     : {failed_rows:,}")


if __name__ == "__main__":
    main()
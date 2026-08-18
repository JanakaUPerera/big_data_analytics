import os
import pandas as pd

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import WriteApi, SYNCHRONOUS
from influxdb_client import Point

CSV_PATH = ""
    
INFLUX_URL = ""
INFLUX_BROWSER_URL = ""
INFLUX_ORG = ""
INFLUX_BUCKET = ""
INFLUX_TOKEN = ""

MEASUREMENT = "hourly_climate"

TAG_COLUMNS = [
    "location"
]

FIELD_COLUMNS = [
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "weather_code",
    "surface_pressure",
    "cloud_cover",
    "wind_gusts_10m",
    "latitude",
    "longitude"
]

BATCH_SIZE = 5000

def initiating_influxdb():
    load_dotenv()

    global CSV_PATH, INFLUX_BROWSER_URL, INFLUX_ORG, INFLUX_BUCKET, INFLUX_TOKEN

    CSV_PATH = "data/alaska_fairbanks_airports_hourly_climate.csv"

    INFLUX_BROWSER_URL = os.getenv("INFLUX_BROWSER_URL")
    INFLUX_ORG = os.getenv("INFLUX_ORG")
    INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")
    INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")

    print("Starting InfluxDB ingestion")
    print("CSV:", CSV_PATH)
    print("Influx Browser URL:", INFLUX_BROWSER_URL)
    print("Organization:", INFLUX_ORG)
    print("Bucket:", INFLUX_BUCKET)
    print("Token loaded:", bool(INFLUX_TOKEN))

def load_dataset() -> pd.DataFrame:
    # Load the CSV and inspect the timestamp parsing
    df = pd.read_csv(CSV_PATH)

    df["date"] = pd.to_datetime(df["date"], utc=True)

    print("\nRows:", len(df))
    print("First timestamp:", df["date"].iloc[0])
    print("Last timestamp:", df["date"].iloc[-1])
    print("\nColumns:")
    print(df.columns.tolist())
    
    return df

def check_data_quality(df: pd.DataFrame) :
    # Check for missing values
    print("\nMissing values per column:")
    dfNull = df.isnull()
    print(dfNull.sum())
    if dfNull.any().any():
        raise ValueError("Data quality check failed: missing values found")

    # Check for duplicate timestamps
    dfDuplicated = df["date"].duplicated()
    duplicate_count = dfDuplicated.sum()
    print("\nDuplicate timestamps:", duplicate_count)
    if dfDuplicated.any():
        raise ValueError("Data quality check failed: duplicate timestamps found")

    # Check whether the hourly sequence has any gaps
    time_diff = df["date"].sort_values().diff()
    dfTimeGaps = (time_diff > pd.Timedelta(hours=1))
    gap_count = dfTimeGaps.sum()
    print("\nHourly gaps:", gap_count)
    if dfTimeGaps.any():
        raise ValueError("Data quality check failed: hourly gaps found")
    
    print("All data quality checks passed.")

def add_location_metadata(df: pd.DataFrame) :
    # Add location metadata for Fairbanks
    df["location"] = "Fairbanks"
    df["latitude"] = 64.85062
    df["longitude"] = -147.67957

    print("\nLocation metadata:")
    print(df[["location", "latitude", "longitude"]].head(1))

def add_influxdb_client_connection() -> tuple[InfluxDBClient, WriteApi]:
    client = InfluxDBClient(
        url=INFLUX_BROWSER_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG
    )

    write_api = client.write_api(write_options=SYNCHRONOUS)
    print("\nWrite API initialized successfully")
    
    return client, write_api

def create_point(row) -> Point :
    point = (
        Point(MEASUREMENT)
        .tag("location", row.location)
        .field("temperature_2m", float(row.temperature_2m))
        .field("precipitation", float(row.precipitation))
        .field("wind_speed_10m", float(row.wind_speed_10m))
        .field("wind_direction_10m", float(row.wind_direction_10m))
        .field("weather_code", int(row.weather_code))
        .field("surface_pressure", float(row.surface_pressure))
        .field("cloud_cover", int(row.cloud_cover))
        .field("wind_gusts_10m", float(row.wind_gusts_10m))
        .field("latitude", float(row.latitude))
        .field("longitude", float(row.longitude))
        .time(row.date)
    )
    
    return point

def write_dataset(df: pd.DataFrame, client: InfluxDBClient, write_api: WriteApi) :
    batch = []

    total_rows = 0
    failed_rows = 0

    for row in df.itertuples():
        try:
            point: Point = create_point(row)
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

def main() :
    initiating_influxdb()
    df = load_dataset()
    check_data_quality(df)
    add_location_metadata(df)
    
    # InfluxDB measurement and field structure
    print("\nMeasurement:", MEASUREMENT)
    print("Tags:", TAG_COLUMNS)
    print("Fields:", FIELD_COLUMNS)
    
    client, write_api = add_influxdb_client_connection()
    write_dataset(df=df, client=client, write_api=write_api)
    

if __name__ == "__main__":
    main()
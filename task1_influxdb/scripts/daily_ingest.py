"""Fetch hourly Fairbanks climate data and write it directly to InfluxDB.

Examples:
    python scripts/daily_ingest.py
    python scripts/daily_ingest.py --start-date 2024-01-01 --end-date 2024-01-31
"""

import argparse
import os
from datetime import date, datetime, timedelta

import openmeteo_requests
import pandas as pd
import requests_cache
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from retry_requests import retry


MEASUREMENT = "hourly_climate"
BATCH_SIZE = 5_000
LATITUDE = 64.85062
LONGITUDE = -147.67957
LOCATION = "Fairbanks"
HOURLY_VARIABLES = [
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "weather_code",
    "surface_pressure",
    "cloud_cover",
    "wind_gusts_10m",
]
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def parse_date(value: str) -> date:
    """Parse an ISO-8601 calendar date supplied on the command line."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Dates must use YYYY-MM-DD format") from exc


def parse_arguments() -> tuple[date, date]:
    parser = argparse.ArgumentParser(
        description="Fetch a date range from Open-Meteo and ingest it into InfluxDB."
    )
    yesterday = date.today() - timedelta(days=1)
    parser.add_argument(
        "--start-date", type=parse_date, default=yesterday,
        help="First date to ingest (inclusive). Defaults to yesterday.",
    )
    parser.add_argument(
        "--end-date", type=parse_date, default=yesterday,
        help="Last date to ingest (inclusive). Defaults to yesterday.",
    )
    args = parser.parse_args()

    if args.start_date > args.end_date:
        parser.error("--start-date must be on or before --end-date")
    if args.end_date >= date.today():
        parser.error("--end-date must be before today because archive data is used")

    return args.start_date, args.end_date


def fetch_climate_data(start_date: date, end_date: date) -> pd.DataFrame:
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    response = openmeteo.weather_api(
        ARCHIVE_URL,
        params={
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": HOURLY_VARIABLES,
            "timezone": "auto",
        },
    )[0]

    hourly = response.Hourly()
    data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
    }
    for index, variable in enumerate(HOURLY_VARIABLES):
        data[variable] = hourly.Variables(index).ValuesAsNumpy()

    dataframe = pd.DataFrame(data)
    dataframe["location"] = LOCATION
    dataframe["latitude"] = LATITUDE
    dataframe["longitude"] = LONGITUDE
    return dataframe


def validate_data(dataframe: pd.DataFrame, start_date: date, end_date: date) -> None:
    expected_rows = ((end_date - start_date).days + 1) * 24
    if len(dataframe) != expected_rows:
        raise ValueError(f"Expected {expected_rows} hourly rows, received {len(dataframe)}")
    if dataframe[HOURLY_VARIABLES].isnull().any().any():
        raise ValueError("Open-Meteo returned missing values; no data was written")
    if dataframe["date"].duplicated().any():
        raise ValueError("Open-Meteo returned duplicate timestamps; no data was written")
    if (dataframe["date"].sort_values().diff().dropna() != pd.Timedelta(hours=1)).any():
        raise ValueError("Open-Meteo returned gaps in hourly timestamps; no data was written")


def create_point(row) -> Point:
    return (
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


def write_data(dataframe: pd.DataFrame) -> None:
    load_dotenv()
    influx_url = os.getenv("INFLUX_BROWSER_URL")
    influx_org = os.getenv("INFLUX_ORG")
    influx_bucket = os.getenv("INFLUX_BUCKET")
    influx_token = os.getenv("INFLUX_TOKEN")
    if not all([influx_url, influx_org, influx_bucket, influx_token]):
        raise ValueError("Set INFLUX_BROWSER_URL, INFLUX_ORG, INFLUX_BUCKET, and INFLUX_TOKEN in .env")

    with InfluxDBClient(url=influx_url, token=influx_token, org=influx_org) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        points = [create_point(row) for row in dataframe.itertuples(index=False)]
        for start in range(0, len(points), BATCH_SIZE):
            write_api.write(
                bucket=influx_bucket,
                org=influx_org,
                record=points[start:start + BATCH_SIZE],
            )
            print(f"Inserted {min(start + BATCH_SIZE, len(points)):,}/{len(points):,} records")


def main() -> None:
    start_date, end_date = parse_arguments()
    print(f"Fetching climate data from {start_date} to {end_date} (inclusive)")
    dataframe = fetch_climate_data(start_date, end_date)
    validate_data(dataframe, start_date, end_date)
    write_data(dataframe)
    print(f"Ingestion completed: {len(dataframe):,} hourly records written to {MEASUREMENT}")


if __name__ == "__main__":
    main()

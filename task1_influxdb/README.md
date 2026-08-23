# Task 1: Distributed Time-Series Data Management with InfluxDB

This folder contains the InfluxDB implementation for Task 1 of the Big Data Analytics Technologies coursework. It provisions InfluxDB 2.7 with Docker Compose, ingests hourly Fairbanks climate observations, and provides Flux queries for aggregation, moving averages, anomaly detection, and downsampling.

## Contents

```text
task1_influxdb/
|-- docker-compose.yml                         InfluxDB and Python services
|-- .env.example                               Configuration template
|-- requirements.txt                            Python ingestion dependencies
|-- data/alaska_fairbanks_airports_hourly_climate.csv
|-- scripts/download_data.py                    Download/refresh the CSV
|-- scripts/ingest_climate.py                   Ingest the historical CSV
|-- scripts/daily_ingest.py                     Optional Open-Meteo refresh
|-- queries/task1_influxdb.flux                  Flux analysis examples
|-- evidence/                                   Place screenshots and exports here
`-- influxdb-data/                              Persistent InfluxDB data
```

## Prerequisites

- Docker Desktop with Docker Compose enabled
- PowerShell, Bash, or an equivalent terminal
- Internet access if downloading the dataset or using `daily_ingest.py`

The practical workload is run through the two services in `docker-compose.yml`. No Python installation on the host is required.

## Configure the deployment

Run the commands from this directory:

```powershell
cd task1_influxdb
Copy-Item .env.example .env
```

Edit `.env` before starting the service. Keep these values consistent:

```dotenv
INFLUXDB_INIT_ORG=bigdata-coursework
INFLUXDB_INIT_BUCKET=fairbanks-climate
INFLUXDB_INIT_ADMIN_TOKEN=replace-with-a-long-random-token
INFLUX_ORG=bigdata-coursework
INFLUX_BUCKET=fairbanks-climate
INFLUX_TOKEN=replace-with-the-same-token
INFLUX_BROWSER_URL=http://localhost:8086
```

The Compose file reads the `INFLUXDB_INIT_*` variables during first-time setup. The Python scripts read the `INFLUX_*` variables. In particular, `INFLUX_BUCKET` must match `INFLUXDB_INIT_BUCKET`, because the Flux queries use `fairbanks-climate`.

Do not commit `.env` or real tokens. The `.env.example` file is safe to share as a template.

## Start InfluxDB

```powershell
docker compose up -d
docker compose ps
docker compose logs --tail=50 influxdb
docker compose logs --tail=50 python
```

Wait until `influxdb` reports `healthy` and `python` is running. The Python service installs the dependencies from `requirements.txt` when it starts. The InfluxDB user interface and API are available at <http://localhost:8086>. The data directory is persisted in `influxdb-data/`, so restarting the container does not remove the database.

Check the health endpoint:

```powershell
Invoke-RestMethod http://localhost:8086/health
```

## Obtain or verify the dataset

The supplied CSV is already in `data/`. To download a fresh copy from the Open-Meteo archive, run the downloader inside the Compose-managed Python service:

```powershell
docker compose exec python python scripts/download_data.py
```

The downloader requests data from 2015-01-01 through yesterday and writes `data/alaska_fairbanks_airports_hourly_climate.csv`.

## Ingest the historical CSV

The ingestion script:

- Parses the CSV `date` column as UTC-aware timestamps.
- Rejects missing values, duplicate timestamps, and gaps longer than one hour.
- Writes the `hourly_climate` measurement in batches of 5,000 points.
- Stores `location=Fairbanks` as a tag.
- Stores climate values and latitude/longitude as fields.
- Uses each source timestamp, rather than the deployment clock.

Run it inside the Compose-managed Python service:

```powershell
docker compose exec python python scripts/ingest_climate.py
```

The Python service receives its InfluxDB connection settings from `.env` through Compose. Do not place the token in this README.

## Optional daily refresh

`daily_ingest.py` fetches a date range from the Open-Meteo archive and writes it directly to the same measurement. The Python service and its dependencies are created by `docker compose up -d`:

```powershell
docker compose up -d
```

Run the script inside the Compose-managed service:

```powershell
docker compose exec python python scripts/daily_ingest.py --start-date 2024-01-01 --end-date 2024-01-31
```

The services must be running and InfluxDB must be healthy. Check them with `docker compose ps`; restart them with `docker compose up -d`. The end date must be before the current date because this script uses the archive API.

## Run and inspect the Flux analyses

Open <http://localhost:8086>, sign in with the credentials from `.env`, and select **Explore**. Open `queries/task1_influxdb.flux`, then execute the queries individually. Update the `range()` dates when the selected period does not match the timestamps currently stored in the bucket.

The query file demonstrates:

1. Daily means, totals, minimums, maximums, and other weather summaries.
2. A three-hour moving average of `temperature_2m` using `movingAverage(n: 3)`.
3. Anomaly isolation for values outside the mean plus or minus two standard deviations.
4. Daily temperature downsampling into `fairbanks-climate-downsampled`.

The downsampling query must be executed after the source data exists. Before running it, create the destination bucket with a 30-day retention policy in the InfluxDB UI, or with the CLI:

```powershell
docker compose exec influxdb influx bucket create `
  --org bigdata-coursework `
  --name fairbanks-climate-downsampled `
  --retention 30d `
  --token $env:INFLUXDB_INIT_ADMIN_TOKEN
```

Query

```flux
from(bucket: "fairbanks-climate") 
  |> range(start: -1d) 
  |> filter(fn: (r) => r._measurement == "hourly_climate") 
  |> filter(fn: (r) => r.location == "Fairbanks") 
  |> filter(fn: (r) => r._field == "temperature_2m") 
  |> aggregateWindow( every: 1d, fn: mean, createEmpty: false ) 
  |> set( key: "_measurement", value: "daily_climate" ) 
  |> to( bucket: "fairbanks-climate-downsampled", org: "bigdata-coursework" )
```

The query currently uses `range(start: -1d)`, which is useful for a recent refresh. For the historical CSV, replace it with a range covering the imported records before executing the `to()` operation.

## Inspect with the InfluxDB CLI

List buckets and confirm the source bucket:

```powershell
docker compose exec influxdb influx bucket list `
  --org bigdata-coursework `
  --token $env:INFLUXDB_INIT_ADMIN_TOKEN
```

The web UI is recommended for running the complete Flux file. It provides the result table, query history, and chart view needed for coursework evidence. Save screenshots or exported results in `evidence/`.

## Stop, restart, and reset

```powershell
docker compose stop
docker compose start
docker compose down
```

`docker compose down` removes the container but preserves `influxdb-data/`. To deliberately recreate the database from an empty state, stop the service and remove that directory only after confirming that existing evidence is backed up:

```powershell
docker compose down
Remove-Item -Recurse -Force .\influxdb-data\*
docker compose up -d
```

Because InfluxDB setup variables are applied during first initialization, changing credentials or the initial bucket after data already exists does not reconfigure the existing database. Reset the persistent directory or change the database through the UI/CLI instead.

# Task 2: Kafka + Flink Streaming Analytics

This folder implements the coursework task for producing traffic telemetry events to Kafka and processing them with a Flink event-time tumbling window job. The architecture is:

- Python producer reads CSV telemetry rows from `data/`
- Kafka topic `traffic-telemetry` stores the stream
- Flink reads from Kafka, assigns event-time watermarks, and aggregates by device and 15-minute tumbling window
- Output is printed to stdout in the running Flink job

## Project layout

```text
task2_kafka_flink/
├── docker-compose.yml
├── data/
│   ├── sh59-i6y9_part1.csv
│   ├── sh59-i6y9_part2.csv
│   ├── sh59-i6y9_part3.csv
│   └── sh59-i6y9_part4.csv
├── flink_job/
│   ├── pom.xml
│   ├── src/main/java/org/coursework/
│   │   ├── TrafficAnalyticsJob.java
│   │   ├── TrafficEvent.java
│   │   └── TrafficWindowResult.java
│   └── target/
├── producer/
│   ├── producer.py
│   ├── preview_message.py
│   ├── inspect_dataset.py
│   └── out_of_order_test.py
├── scripts/
│   └── download_data.py
└── outputs/
```

## Prerequisites

Before running the task, install:

- Docker Desktop with Docker Compose enabled
- Java 21
- Maven 3.9+
- Python 3.10+
- `pip install pandas kafka-python`

## 1) Start the infrastructure

From this folder:

```powershell
cd task2_kafka_flink
docker compose up -d
```

This starts:

- Kafka on `localhost:9192`
- Kafka UI on `http://localhost:8185`
- Flink JobManager on `http://localhost:8184`
- Flink TaskManager

Check the containers:

```powershell
docker compose ps
```

Useful extra checks:

```powershell
docker compose logs --tail=50 kafka
docker compose logs --tail=50 jobmanager
docker compose logs --tail=50 taskmanager
```

## 2) Build the Flink job

From the Flink project directory:

```powershell
cd task2_kafka_flink\flink_job
mvn package
```

This produces a shaded JAR in:

```text
flink_job/target/traffic-analytics-1.0.jar
```

## 3) Create the Kafka topic

Create the topic used by the producer and Flink job:

```powershell
docker exec coursework-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --create `
  --topic traffic-telemetry `
  --partitions 3 `
  --replication-factor 1
```

## 4) Inspect or preview the dataset

Before sending production data, you can inspect a sample record:

```powershell
cd task2_kafka_flink\producer
python .\preview_message.py
```

You can also inspect the dataset size and schema:

```powershell
python .\inspect_dataset.py
```

## 5) Run the Kafka producer

Open a separate terminal and run:

```powershell
cd task2_kafka_flink\producer
python .\producer.py
```

This script:

- reads all `sh59-i6y9_part*.csv` files from `../data`
- creates JSON messages using the CSV columns
- sends each message to Kafka topic `traffic-telemetry`
- flushes after every message
- pauses 2 seconds between records to emulate streaming behaviour

The producer uses the host address `localhost:9192`, which is published by Docker Compose.

## 6) Submit the Flink job

Leave the producer running, then in another terminal copy the built JAR into the Flink JobManager container and submit it:

```powershell
cd task2_kafka_flink
docker compose cp .\flink_job\target\traffic-analytics-1.0.jar jobmanager:/tmp/traffic-analytics-1.0.jar
docker compose exec jobmanager ./bin/flink run -d /tmp/traffic-analytics-1.0.jar
```

The Java job expects Kafka to be reachable at `kafka:9092` from inside the Flink network, and it subscribes to the topic `traffic-telemetry` using event-time processing.

## 7) Observe the output

If the producer and Flink job are both running, the job prints aggregate results similar to:

```text
TrafficWindowResult{atd_device_id=6171, window_start=2017-06-01 01:15:00, window_end=2017-06-01 01:30:00, total_vehicle_count=102}
TrafficWindowResult{atd_device_id=6171, window_start=2017-06-01 01:30:00, window_end=2017-06-01 01:45:00, total_vehicle_count=106}
```

You can inspect the running job in the Flink dashboard at:

- http://localhost:8184

You can inspect Kafka topics and messages through the Kafka UI at:

- http://localhost:8185

## 8) Optional out-of-order test

The folder includes an out-of-order validation script:

```powershell
cd task2_kafka_flink\producer
python .\out_of_order_test.py
```

This is useful to test how the bounded out-of-orderness watermark strategy behaves when timestamps arrive later than expected.

## 9) Stop the environment

To stop the stack:

```powershell
docker compose down
```

To stop and remove all containers, networks, and local state owned by the Compose project:

```powershell
docker compose down --volumes
```

## Notes

- The Kafka producer sends all records in the CSV files, so it can take a while to finish. The `time.sleep(2)` delay is intentional to make the stream visible in Flink.
- The Flink job uses a 15-minute tumbling event-time window and a bounded out-of-orderness watermark of 10 seconds.
- The code is designed to work inside the Docker Compose network, so container-to-container communication uses `kafka:9092`, while the producer connects from the host using `localhost:9192`.

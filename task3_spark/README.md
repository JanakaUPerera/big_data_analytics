# Task 3 - Scalable Analytics with Apache Spark

This folder contains the Dockerized implementation for Task 3 of the Big Data Analytics Technologies coursework.

The project uses Apache Spark 3.5.8 in standalone mode to analyse the SNAP Web-BerkStan directed graph. The PySpark job removes metadata rows, parses source/destination edges, computes destination in-degree, identifies the top 50 destination vertices, and captures execution evidence.

## Project layout

```text
docker-compose.yml              Spark master, two workers, and history server
data/web-BerkStan.txt           SNAP input graph
docs/                           Documentation and summary tables
jobs/berkstan_analysis.py        PySpark analysis job
output/                          CSV results, plan, summary, and event logs
evidence/                        REST API snapshots and driver log
```

## Prerequisites

- Docker Desktop with the Docker Compose plugin
- Docker Desktop running
- The input file `data/web-BerkStan.txt` present before submission

No Python, PySpark, or Spark installation is required on the host. The Spark image supplies the runtime and the job is submitted from the Spark master container.

## Start the cluster

Open PowerShell in this folder:

```powershell
cd task3_spark
docker compose up -d
```

The Compose topology contains:

- `spark-master`: Spark standalone master, 4 worker-visible total cores
- `spark-worker-1`: 2 cores and 2 GB memory
- `spark-worker-2`: 2 cores and 2 GB memory
- `spark-history`: reads Spark event logs from the shared output volume

Check that all services are running:

```powershell
docker compose ps
```

## Run the analysis

Submit the job through the master container:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark/jobs/berkstan_analysis.py
```

The job reads `/opt/spark/data/web-BerkStan.txt`, which is the read-only mount of `data/web-BerkStan.txt` on the host. Results are written to `/opt/spark/output`, and therefore appear in this folder's `output/` directory. The driver log and Spark REST evidence are written to `/opt/spark/evidence`, appearing under `evidence/`.

The job enables Spark event logging and, by default, waits for the History Server to index the completed application before capturing cluster, job, stage, executor, and environment JSON responses.

To run without REST evidence capture, for example when only the analytical outputs are required:

```powershell
docker exec -e CAPTURE_EVIDENCE=false spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark/jobs/berkstan_analysis.py
```

## Web interfaces

Open these addresses on the host while the containers are running:

- Spark Master UI: http://localhost:8180
- Spark History Server: http://localhost:18080
- Worker 1 UI: http://localhost:8181
- Worker 2 UI: http://localhost:8182
- Driver UI, while a job is active: http://localhost:4040

The History Server is the best place to inspect completed applications, stages, task durations, shuffle metrics, and executor activity.

## Generated artefacts

The analysis creates the following files in `output/`:

- `sample_edges.csv`: first ten parsed directed edges
- `top50_indegree.csv`: 50 destination vertices with the highest in-degree
- `partition_distribution.csv`: edge count by input partition
- `indegree_summary_stats.csv`: descriptive statistics for in-degree values
- `physical_plan.txt`: formatted physical plan for the top-50 query
- `execution_summary.txt`: Spark version, application ID, counts, partitions, and runtime
- `spark-events/`: Spark event logs consumed by the History Server

The following files are captured in `evidence/`:

- `spark_submit_log.txt`: complete driver console output
- `master_status.json`: master and worker topology snapshot
- `history_jobs.json`: application job metrics
- `history_stages_summary.json` and `history_stages_detailed.json`: stage and shuffle metrics
- `history_executors.json`: executor-level metrics
- `history_environment.json`: Spark runtime configuration

The existing interpretation of these artefacts is in [docs/EVIDENCE_REPORT.md](docs/EVIDENCE_REPORT.md).

## Stop and clean up

Stop the containers while retaining output and event logs:

```powershell
docker compose down
```

To remove the containers and their attached anonymous Docker resources as well:

```powershell
docker compose down --remove-orphans
```

The project data, results, and evidence are bind-mounted local files, so `docker compose down` does not delete them.

## Troubleshooting

View service logs if a container does not become healthy:

```powershell
docker compose logs spark-master
```

```powershell
docker compose logs spark-worker-1 spark-worker-2 spark-history
```

If the job cannot contact the History Server, the analytical results still complete; the job records a warning and skips only the REST evidence capture. Confirm that `spark-history` is running and rerun the submission if the JSON evidence is required.

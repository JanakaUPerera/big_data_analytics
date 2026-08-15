from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
import csv
import io
import json
import os
import sys
import time
import urllib.error
import urllib.request
from contextlib import redirect_stdout


DATA_PATH = "/opt/spark/data/web-BerkStan.txt"
OUTPUT_PATH = "/opt/spark/output"
EVENT_LOG_PATH = "/opt/spark/output/spark-events"
EVIDENCE_PATH = "/opt/spark/evidence"

# REST endpoints of the other cluster services, reachable over the shared
# Docker network by service name. Overridable via environment variables in
# case the compose topology (hostnames/ports) is ever changed.
MASTER_UI_URL = os.environ.get("SPARK_MASTER_UI_URL", "http://spark-master:8080")
HISTORY_SERVER_URL = os.environ.get("SPARK_HISTORY_SERVER_URL", "http://spark-history:18080")

# Evidence saving option: set CAPTURE_EVIDENCE=false to skip the REST capture
# step entirely (e.g. when running without the spark-history service up).
CAPTURE_EVIDENCE = os.environ.get("CAPTURE_EVIDENCE", "true").strip().lower() not in ("0", "false", "no")
HISTORY_POLL_ATTEMPTS = int(os.environ.get("HISTORY_POLL_ATTEMPTS", "12"))
HISTORY_POLL_INTERVAL_SECONDS = int(os.environ.get("HISTORY_POLL_INTERVAL_SECONDS", "5"))


class _Tee:
    """Duplicates writes across multiple streams (e.g. real stdout + an
    in-memory buffer), so the driver's console output can be saved to disk
    without disabling live output."""

    def __init__(self, *streams):
        self._streams = streams

    def write(self, data):
        for stream in self._streams:
            stream.write(data)

    def flush(self):
        for stream in self._streams:
            stream.flush()


def write_rows_csv(path, header, rows):
    """Write a small, already-collected result set as a single clean CSV file
    (avoids Spark's multi-part output directories for small evidence artifacts)."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def capture_json(url, dest_path):
    """Fetch a JSON document from a Spark REST endpoint and save it verbatim.
    Never raises: evidence capture must not fail the analytics job itself."""
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
        with open(dest_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"  saved {os.path.basename(dest_path)} <- {url}")
        return True
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError) as exc:
        print(f"  WARNING: failed to capture {url}: {exc}")
        return False


def wait_for_history_server_app(app_id):
    """Poll the History Server until it has indexed this application's event
    log (it reads the log directory on its own periodic refresh interval)."""
    url = f"{HISTORY_SERVER_URL}/api/v1/applications"
    for attempt in range(1, HISTORY_POLL_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                apps = json.loads(response.read().decode("utf-8"))
            if any(a.get("id") == app_id for a in apps):
                print(f"  History Server indexed {app_id} (attempt {attempt}/{HISTORY_POLL_ATTEMPTS}).")
                return True
        except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError) as exc:
            print(f"  History Server not ready yet (attempt {attempt}/{HISTORY_POLL_ATTEMPTS}): {exc}")
        time.sleep(HISTORY_POLL_INTERVAL_SECONDS)
    return False


def capture_cluster_evidence(app_id):
    """Pull cluster/DAG/stage/shuffle/executor evidence straight from Spark's
    own Master and History Server REST APIs (the same data their web UIs
    render) and save it under /opt/spark/evidence."""
    print("\n" + "=" * 70)
    print("CAPTURING CLUSTER EVIDENCE (Master + History Server REST APIs)")
    print("=" * 70)

    os.makedirs(EVIDENCE_PATH, exist_ok=True)

    capture_json(f"{MASTER_UI_URL}/json/", os.path.join(EVIDENCE_PATH, "master_status.json"))

    if not wait_for_history_server_app(app_id):
        print(
            f"  WARNING: {app_id} was not indexed by the History Server within "
            f"{HISTORY_POLL_ATTEMPTS * HISTORY_POLL_INTERVAL_SECONDS}s; "
            f"skipping History Server evidence capture."
        )
        return

    base = f"{HISTORY_SERVER_URL}/api/v1/applications/{app_id}"
    capture_json(f"{base}/jobs", os.path.join(EVIDENCE_PATH, "history_jobs.json"))
    capture_json(f"{base}/stages", os.path.join(EVIDENCE_PATH, "history_stages_summary.json"))
    capture_json(f"{base}/stages?details=true", os.path.join(EVIDENCE_PATH, "history_stages_detailed.json"))
    capture_json(f"{base}/executors", os.path.join(EVIDENCE_PATH, "history_executors.json"))
    capture_json(f"{base}/environment", os.path.join(EVIDENCE_PATH, "history_environment.json"))

    print(f"Cluster evidence written to {EVIDENCE_PATH}")


def main():

    os.makedirs(EVENT_LOG_PATH, exist_ok=True)
    os.makedirs(EVIDENCE_PATH, exist_ok=True)

    # Tee all console output to an in-memory buffer as well as the real
    # stdout, so the full driver log can be saved as evidence without
    # relying on the caller redirecting spark-submit's output to a file.
    log_buffer = io.StringIO()
    original_stdout = sys.stdout
    sys.stdout = _Tee(original_stdout, log_buffer)

    try:
        # Create Spark Session
        # Event logging is enabled so a Spark History Server can serve stage/DAG/
        # shuffle evidence after this driver process (and its port-4040 UI) exits.
        spark = (
            SparkSession.builder
            .appName("Web-BerkStan-InDegree-Analysis")
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", f"file:{EVENT_LOG_PATH}")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        app_id = spark.sparkContext.applicationId

        print("=" * 70)
        print("WEB-BERKSTAN DISTRIBUTED GRAPH ANALYSIS")
        print("=" * 70)

        print(f"Spark version   : {spark.version}")
        print(f"Master          : {spark.sparkContext.master}")
        print(f"Application ID  : {app_id}")

        start_time = time.time()

        # Load raw text using Spark
        # Spark transformations are lazily evaluated.
        raw_df = spark.read.text(DATA_PATH)

        print("\nRaw DataFrame schema:")
        raw_df.printSchema()

        print("\nInitial partitions:")
        print(raw_df.rdd.getNumPartitions())

        # Remove SNAP metadata/header records
        # Header rows begin with '#'.
        clean_df = (
            raw_df
            .filter(~F.col("value").startswith("#"))
            .filter(F.trim(F.col("value")) != "")
        )

        # Split each edge into source and destination vertices
        # SNAP file is whitespace/tab separated.
        edges_df = (
            clean_df
            .select(
                F.split(
                    F.trim(F.col("value")),
                    r"\s+"
                ).alias("parts")
            )
            .filter(F.size(F.col("parts")) >= 2)
            .select(
                F.col("parts")[0]
                    .cast("long")
                    .alias("source"),

                F.col("parts")[1]
                    .cast("long")
                    .alias("destination")
            )
            .filter(
                F.col("source").isNotNull()
                & F.col("destination").isNotNull()
            )
        )

        # Cache parsed graph edges
        # The edge DataFrame is reused in multiple actions.
        edges_df = edges_df.persist(StorageLevel.MEMORY_AND_DISK)

        # First action materialises the cached DataFrame.
        edge_count = edges_df.count()

        print("\n" + "=" * 70)
        print("DATASET INFORMATION")
        print("=" * 70)

        print(f"Valid directed edges : {edge_count:,}")
        print(f"Partitions           : {edges_df.rdd.getNumPartitions()}")

        print("\nSample graph edges:")
        edges_df.show(10, truncate=False)

        sample_edges = [
            (row.source, row.destination)
            for row in edges_df.limit(10).collect()
        ]

        # Calculate in-degree
        # Each destination is grouped and incoming edges counted.
        # This introduces a shuffle.
        indegree_df = (
            edges_df
            .groupBy("destination")
            .agg(
                F.count("*").alias("in_degree")
            )
        )

        # Cache because we use this result more than once.
        indegree_df = indegree_df.persist(
            StorageLevel.MEMORY_AND_DISK
        )

        unique_destination_count = indegree_df.count()

        print("\nUnique destination vertices:")
        print(f"{unique_destination_count:,}")

        # Find Top 50 destination nodes
        top50_df = (
            indegree_df
            .orderBy(
                F.desc("in_degree"),
                F.asc("destination")
            )
            .limit(50)
        )

        print("\n" + "=" * 70)
        print("TOP 50 NODES BY IN-DEGREE")
        print("=" * 70)

        top50_df.show(50, truncate=False)

        top50_rows = [
            (row.destination, row.in_degree)
            for row in top50_df.collect()
        ]

        # Display execution plan
        # Useful evidence for discussing Spark optimization/DAG.
        print("\n" + "=" * 70)
        print("PHYSICAL EXECUTION PLAN")
        print("=" * 70)

        top50_df.explain(mode="formatted")

        plan_buffer = io.StringIO()
        with redirect_stdout(plan_buffer):
            top50_df.explain(mode="formatted")
        physical_plan_text = plan_buffer.getvalue()

        # Basic partition analysis for possible skew
        partition_distribution = (
            edges_df
            .withColumn(
                "partition_id",
                F.spark_partition_id()
            )
            .groupBy("partition_id")
            .count()
            .orderBy("partition_id")
        )

        print("\n" + "=" * 70)
        print("EDGE DISTRIBUTION ACROSS PARTITIONS")
        print("=" * 70)

        partition_distribution.show(
            100,
            truncate=False
        )

        partition_rows = [
            (row.partition_id, row["count"])
            for row in partition_distribution.collect()
        ]

        # Statistical information about in-degree distribution
        print("\n" + "=" * 70)
        print("IN-DEGREE DISTRIBUTION STATISTICS")
        print("=" * 70)

        summary_df = indegree_df.select("in_degree").summary()
        summary_df.show()

        summary_rows = [
            (row.summary, row.in_degree)
            for row in summary_df.collect()
        ]

        end_time = time.time()
        elapsed_seconds = end_time - start_time

        print("\n" + "=" * 70)
        print("EXECUTION SUMMARY")
        print("=" * 70)

        print(
            f"Approximate application execution time: "
            f"{elapsed_seconds:.2f} seconds"
        )

        # Persist result artefacts to the shared /opt/spark/output volume
        # so they survive after this driver process exits.
        write_rows_csv(
            os.path.join(OUTPUT_PATH, "sample_edges.csv"),
            ["source", "destination"],
            sample_edges,
        )
        write_rows_csv(
            os.path.join(OUTPUT_PATH, "top50_indegree.csv"),
            ["destination", "in_degree"],
            top50_rows,
        )
        write_rows_csv(
            os.path.join(OUTPUT_PATH, "partition_distribution.csv"),
            ["partition_id", "edge_count"],
            partition_rows,
        )
        write_rows_csv(
            os.path.join(OUTPUT_PATH, "indegree_summary_stats.csv"),
            ["summary", "in_degree"],
            summary_rows,
        )

        with open(
            os.path.join(OUTPUT_PATH, "physical_plan.txt"),
            "w",
            encoding="utf-8",
        ) as f:
            f.write(physical_plan_text)

        with open(
            os.path.join(OUTPUT_PATH, "execution_summary.txt"),
            "w",
            encoding="utf-8",
        ) as f:
            f.write(f"Spark version              : {spark.version}\n")
            f.write(f"Master                     : {spark.sparkContext.master}\n")
            f.write(f"Application ID             : {app_id}\n")
            f.write(f"Application name           : {spark.sparkContext.appName}\n")
            f.write(f"Default parallelism        : {spark.sparkContext.defaultParallelism}\n")
            f.write(f"Valid directed edges       : {edge_count}\n")
            f.write(f"Unique destination vertices: {unique_destination_count}\n")
            f.write(f"Edge partitions            : {edges_df.rdd.getNumPartitions()}\n")
            f.write(f"Driver execution time (s)  : {elapsed_seconds:.2f}\n")

        print(f"\nResult artefacts written to {OUTPUT_PATH}")

        # Cleanup
        indegree_df.unpersist()
        edges_df.unpersist()

        spark.stop()

        # Cluster/DAG/shuffle/skew evidence, pulled straight from Spark's own
        # Master and History Server REST APIs now that the run has finished
        # and its event log is finalised.
        if CAPTURE_EVIDENCE:
            capture_cluster_evidence(app_id)
        else:
            print("\nCAPTURE_EVIDENCE is disabled (env var); skipping REST evidence capture.")
    finally:
        sys.stdout = original_stdout
        log_path = os.path.join(EVIDENCE_PATH, "spark_submit_log.txt")
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(log_buffer.getvalue())
        print(f"Driver console log saved to {log_path}")


if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
import csv
import io
import os
import time
from contextlib import redirect_stdout


DATA_PATH = "/opt/spark/data/web-BerkStan.txt"
OUTPUT_PATH = "/opt/spark/output"
EVENT_LOG_PATH = "/opt/spark/output/spark-events"


def write_rows_csv(path, header, rows):
    """Write a small, already-collected result set as a single clean CSV file
    (avoids Spark's multi-part output directories for small evidence artifacts)."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def main():

    os.makedirs(EVENT_LOG_PATH, exist_ok=True)

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

    print("=" * 70)
    print("WEB-BERKSTAN DISTRIBUTED GRAPH ANALYSIS")
    print("=" * 70)

    print(f"Spark version   : {spark.version}")
    print(f"Master          : {spark.sparkContext.master}")
    print(f"Application ID  : {spark.sparkContext.applicationId}")

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

    # Persist evidence artefacts to the shared /opt/spark/output volume
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
        f.write(f"Application ID             : {spark.sparkContext.applicationId}\n")
        f.write(f"Application name           : {spark.sparkContext.appName}\n")
        f.write(f"Default parallelism        : {spark.sparkContext.defaultParallelism}\n")
        f.write(f"Valid directed edges       : {edge_count}\n")
        f.write(f"Unique destination vertices: {unique_destination_count}\n")
        f.write(f"Edge partitions            : {edges_df.rdd.getNumPartitions()}\n")
        f.write(f"Driver execution time (s)  : {elapsed_seconds:.2f}\n")

    print(f"\nEvidence artefacts written to {OUTPUT_PATH}")

    # Cleanup
    indegree_df.unpersist()
    edges_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
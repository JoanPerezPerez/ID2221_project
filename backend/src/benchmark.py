"""Phase 3: matched data scan benchmark (nested JSON vs Silver Parquet).

Benchmarks complete Spark actions; pre-specified schema avoids JSON schema inference.
No Spark cache is used. This does NOT guarantee cold OS/filesystem caches.
"""
from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from etl import ROOT_SCHEMA


def scan_json(spark, path):
    rows = (
        spark.read.option("multiLine", "true")
        .schema(ROOT_SCHEMA).json(path)
        .select(F.explode("playlists").alias("p"))
        .select(F.explode("p.tracks").alias("t"))
        .select(F.col("t.track_uri").alias("track_uri"))
    )
    return scan_result(rows)


def scan_parquet(spark, path):
    rows = spark.read.parquet(path).select("track_uri")
    return scan_result(rows)


def scan_result(rows):
    # Forces payload decoding instead of only a metadata count.
    r = (
        rows.filter(F.col("track_uri").isNotNull() & (F.length(F.trim("track_uri")) > 0))
        .agg(F.count("*").alias("count"), F.sum(F.length("track_uri")).alias("sum_uri_lengths"))
        .first()
    )
    return int(r["count"]), int(r["sum_uri_lengths"] or 0)


def storage_size_bytes(spark, expression):
    """Sum Hadoop FileSystem file lengths (works for local and HDFS)."""
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(expression)
    fs = hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    statuses = fs.globStatus(hpath)
    if statuses is None:
        return None
    total = 0
    for status in statuses:
        if status.isFile():
            name = status.getPath().getName()
            if not name.startswith(("_", ".")):
                total += status.getLen()
        else:
            files = fs.listFiles(status.getPath(), True)
            while files.hasNext():
                f = files.next()
                name = f.getPath().getName()
                if not name.startswith(("_", ".")):
                    total += f.getLen()
    return total


def main():
    p = argparse.ArgumentParser(description="MPD Phase 3 JSON vs Parquet benchmark")
    p.add_argument("--raw", required=True, help="JSON file glob; local or HDFS URI")
    p.add_argument("--processed", default="data/processed", help="ETL output root")
    p.add_argument("--spark-master", default="local[4]")
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--report-json", default="reports/benchmark.json", help="Local path on driver")
    args = p.parse_args()
    if args.runs < 1 or args.warmup < 0:
        p.error("--runs must be >=1 and --warmup >=0")

    spark = SparkSession.builder.appName("MPD-Benchmark").master(args.spark_master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        parquet_path = args.processed.rstrip("/") + "/silver/playlist_tracks"
        # Alternate format order across independent iterations to reduce order bias.
        jobs = {"json": lambda: scan_json(spark, args.raw),
                "parquet": lambda: scan_parquet(spark, parquet_path)}
        for _ in range(args.warmup):
            for fn in jobs.values():
                fn()
        timings = {name: [] for name in jobs}
        results = {name: [] for name in jobs}
        for run in range(args.runs):
            order = ["json", "parquet"] if run % 2 == 0 else ["parquet", "json"]
            for name in order:
                start = time.perf_counter()
                result = jobs[name]()
                timings[name].append(round(time.perf_counter() - start, 6))
                results[name].append(result)
        if len(set(results["json"] + results["parquet"])) != 1:
            raise AssertionError(
                "JSON and Parquet are not the same dataset or differ in the scan result; "
                "re-run ETL using exactly --raw as input before benchmarking."
            )
        med_json = statistics.median(timings["json"])
        med_parquet = statistics.median(timings["parquet"])
        json_bytes = storage_size_bytes(spark, args.raw)
        relation_bytes = storage_size_bytes(spark, parquet_path)
        silver_bytes = [storage_size_bytes(spark, args.processed.rstrip("/") + "/silver/" + table)
                        for table in ("playlists", "tracks", "playlist_tracks")]
        report = {
            "input_json": args.raw,
            "input_parquet": parquet_path,
            "raw_json_bytes": json_bytes,
            "silver_playlist_tracks_parquet_bytes": relation_bytes,
            "all_silver_tables_parquet_bytes": (sum(silver_bytes) if all(x is not None for x in silver_bytes) else None),
            "size_note": "Raw JSON includes all MPD metadata; Silver tables are transformed/restructured. These sizes do not isolate format-only compression.",
            "runs": args.runs,
            "warmup": args.warmup,
            "json_times_s": timings["json"],
            "parquet_times_s": timings["parquet"],
            "median_json_s": med_json,
            "median_parquet_s": med_parquet,
            "speedup_json_over_parquet": med_json / med_parquet if med_parquet else None,
            "matching_scan_result": {"rows": results["json"][0][0], "sum_uri_lengths": results["json"][0][1]},
            "methodology": "Equivalent decoded-URI scan; JSON includes nested-array explode; Parquet is pre-flattened; caches and I/O are not cold-controlled.",
        }
        print("\n=== Phase 3: read benchmark ===")
        print(json.dumps(report, indent=2))
        destination = Path(args.report_json)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"Local report: {destination}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

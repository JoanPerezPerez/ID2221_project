"""Phase 2: MPD exploratory analysis from Parquet Silver/Gold tables.

All large aggregations are performed in Spark. Outputs are Spark CSV/JSON
DIRECTORIES, so --report-output may also be an HDFS URI.
"""
from __future__ import annotations

import argparse
import json

from pyspark.sql import SparkSession, functions as F


def main() -> None:
    parser = argparse.ArgumentParser(description="MPD Phase 2: exploratory data analysis")
    parser.add_argument("--input", default="data/processed", help="ETL output root")
    parser.add_argument("--report-output", default="reports/eda", help="Spark output root")
    parser.add_argument("--spark-master", default="local[4]")
    parser.add_argument("--top-n", type=int, default=20)
    args = parser.parse_args()
    if args.top_n <= 0:
        parser.error("--top-n must be positive")

    spark = SparkSession.builder.appName("MPD-EDA").master(args.spark_master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    base = args.input.rstrip("/")
    out = args.report_output.rstrip("/")
    try:
        p = spark.read.parquet(f"{base}/silver/playlists")
        t = spark.read.parquet(f"{base}/silver/tracks")
        pt = spark.read.parquet(f"{base}/silver/playlist_tracks")
        x = spark.read.parquet(f"{base}/gold/interactions_uri")

        n_playlists = p.count()        # includes empty playlists
        n_tracks = t.count()
        n_occurrences = pt.count()     # includes duplicate occurrences within a playlist
        n_interactions = x.count()     # unique playlist-track links (ALS matrix nonzeros)
        if not n_playlists or not n_tracks:
            raise ValueError("No playlists/tracks in the processed dataset.")

        sizes = (
            x.groupBy("playlist_id").agg(F.count("*").alias("unique_tracks"))
            .join(p.select("playlist_id"), "playlist_id", "right")
            .fillna({"unique_tracks": 0})
        )
        quantiles = sizes.approxQuantile("unique_tracks", [0.25, 0.5, 0.75, 0.9, 0.99], 0.01)
        summary_row = sizes.agg(
            F.min("unique_tracks").alias("min"),
            F.avg("unique_tracks").alias("mean"),
            F.max("unique_tracks").alias("max"),
        ).first()

        distribution = (
            sizes.withColumn(
                "bucket",
                F.when(F.col("unique_tracks") == 0, "00: 0")
                .when(F.col("unique_tracks") <= 5, "01: 1-5")
                .when(F.col("unique_tracks") <= 10, "02: 6-10")
                .when(F.col("unique_tracks") <= 25, "03: 11-25")
                .when(F.col("unique_tracks") <= 50, "04: 26-50")
                .when(F.col("unique_tracks") <= 100, "05: 51-100")
                .when(F.col("unique_tracks") <= 200, "06: 101-200")
                .otherwise("07: 201+"),
            )
            .groupBy("bucket")
            .agg(F.count("*").alias("playlist_count"))
            .orderBy("bucket")
        )

        popularity = (
            x.groupBy("track_uri")
            .agg(F.count("*").alias("playlist_count"))
            .join(t.select("track_uri", "track_name", "artist_name"), "track_uri", "left")
            .orderBy(F.desc("playlist_count"), F.asc("track_uri"))
        )
        artist_popularity = (
            x.join(t.select("track_uri", "artist_uri", "artist_name"), "track_uri", "left")
            .filter(F.col("artist_uri").isNotNull())
            .groupBy("artist_uri", "artist_name")
            .agg(F.count("*").alias("playlist_track_interactions"))
            .orderBy(F.desc("playlist_track_interactions"), F.asc("artist_uri"))
        )

        density = n_interactions / (n_playlists * n_tracks)
        stats = {
            "playlist_count": n_playlists,
            "unique_tracks": n_tracks,
            "track_occurrences": n_occurrences,
            "unique_playlist_track_interactions": n_interactions,
            "duplicates_within_playlists": n_occurrences - n_interactions,
            "matrix_possible_entries": n_playlists * n_tracks,
            "matrix_density": density,
            "matrix_sparsity": 1.0 - density,
            "playlist_unique_track_count": {
                "min": summary_row["min"],
                "mean": summary_row["mean"],
                "max": summary_row["max"],
                "p25": quantiles[0], "median": quantiles[1],
                "p75": quantiles[2], "p90": quantiles[3], "p99": quantiles[4],
            },
            "note": "sizes use distinct tracks; empty playlists included; popularity counts playlists, not repeated occurrences",
        }
        print("\n=== Phase 2: EDA summary ===")
        print(json.dumps(stats, indent=2, ensure_ascii=False))
        print("\nPlaylist-size distribution:")
        distribution.show(truncate=False)
        print("\nMost frequent tracks (by playlists containing them):")
        popularity.show(args.top_n, truncate=38)

        distribution.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/playlist_size_buckets")
        popularity.limit(args.top_n).coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/top_tracks")
        artist_popularity.limit(args.top_n).coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/top_artists")
        spark.createDataFrame([(json.dumps(stats),)], ["summary_json"]).coalesce(1).write.mode("overwrite").json(f"{out}/summary")
        print(f"\nReports saved in: {out}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

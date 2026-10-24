"""Basic data-quality checks for completed Spotify MPD ETL."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/processed")
    parser.add_argument("--spark-master", default="local[4]")
    parser.add_argument("--expected-playlists", type=int, default=None)
    parser.add_argument("--skip-index", action="store_true")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("MPD-Validation").master(args.spark_master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    base = args.output.rstrip("/")

    try:
        p = spark.read.parquet(f"{base}/silver/playlists")
        t = spark.read.parquet(f"{base}/silver/tracks")
        pt = spark.read.parquet(f"{base}/silver/playlist_tracks")
        i = spark.read.parquet(f"{base}/gold/interactions_uri")

        counts = {
            "playlists": p.count(),
            "tracks": t.count(),
            "playlist_track_occurrences": pt.count(),
            "unique_implicit_interactions": i.count(),
        }
        for k, v in counts.items():
            print(f"{k}: {v:,}")
        assert counts["playlists"] > 0, "No playlists were read"
        if args.expected_playlists is not None:
            assert counts["playlists"] == args.expected_playlists, "Playlist count mismatch"

        assert p.filter(F.col("playlist_id").isNull()).limit(1).count() == 0
        assert t.filter(F.col("track_uri").isNull()).limit(1).count() == 0
        assert i.filter(F.col("rating") != 1.0).limit(1).count() == 0
        assert i.groupBy("playlist_id", "track_uri").count().filter("count > 1").limit(1).count() == 0
        assert pt.join(p.select("playlist_id"), "playlist_id", "left_anti").limit(1).count() == 0
        assert pt.join(t.select("track_uri"), "track_uri", "left_anti").limit(1).count() == 0

        if not args.skip_index:
            indexed = spark.read.parquet(f"{base}/gold/als_interactions")
            mapping = spark.read.parquet(f"{base}/gold/track_mapping")
            assert indexed.count() == counts["unique_implicit_interactions"]
            assert mapping.count() == counts["tracks"]
            assert mapping.groupBy("track_id").count().filter("count > 1").limit(1).count() == 0
            print("ALS input example:")
            indexed.orderBy("playlist_id", "track_id").show(10, truncate=False)

        print("All data-quality checks passed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

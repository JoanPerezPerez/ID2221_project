"""Spotify MPD ETL: nested JSON -> Parquet tables -> ALS-ready interactions.

Run from the project root, e.g.:
  python src/etl.py --input 'data/sample/mpd.slice.*.json' --output data/processed
"""

from __future__ import annotations

import argparse

from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


TRACK_SCHEMA = StructType(
    [
        StructField("pos", IntegerType(), True),
        StructField("track_uri", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("artist_uri", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("album_uri", StringType(), True),
        StructField("album_name", StringType(), True),
        StructField("duration_ms", LongType(), True),
    ]
)
PLAYLIST_SCHEMA = StructType(
    [
        StructField("pid", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("num_tracks", IntegerType(), True),
        StructField("num_followers", IntegerType(), True),
        StructField("modified_at", LongType(), True),
        StructField("tracks", ArrayType(TRACK_SCHEMA), True),
    ]
)
ROOT_SCHEMA = StructType(
    [
        StructField("playlists", ArrayType(PLAYLIST_SCHEMA), True),
    ]
)


def build_tables(spark: SparkSession, path: str):
    # One multi-line JSON document per MPD slice; each document has a playlists array.
    source = (
        spark.read.option("multiLine", "true")
        .schema(ROOT_SCHEMA)
        .json(path)
    )

    flat_playlists = (
        source.select(F.explode("playlists").alias("playlist"))
        .filter(F.col("playlist.pid").isNotNull())
    )

    playlists = flat_playlists.select(
        F.col("playlist.pid").alias("playlist_id"),
        F.col("playlist.name").alias("name"),
        F.col("playlist.num_tracks").alias("num_tracks"),
        F.col("playlist.num_followers").alias("num_followers"),
        F.col("playlist.modified_at").alias("modified_at"),
    ).dropDuplicates(["playlist_id"])

    track_rows = (
        flat_playlists.select(
            F.col("playlist.pid").alias("playlist_id"),
            F.explode_outer("playlist.tracks").alias("track"),
        )
        .filter(F.col("track.track_uri").isNotNull())
        .filter(F.length(F.trim(F.col("track.track_uri"))) > 0)
        .select(
            "playlist_id",
            F.col("track.pos").alias("position"),
            F.col("track.track_uri").alias("track_uri"),
            F.col("track.track_name").alias("track_name"),
            F.col("track.artist_uri").alias("artist_uri"),
            F.col("track.artist_name").alias("artist_name"),
            F.col("track.album_uri").alias("album_uri"),
            F.col("track.album_name").alias("album_name"),
            F.col("track.duration_ms").alias("duration_ms"),
        )
    )

    # Silver: preserve every occurrence and the original track order.
    playlist_tracks = track_rows.select(
        "playlist_id", "track_uri", "position"
    )

    # Silver: one row per track URI. MIN is deterministic if labels differ across slices.
    tracks = track_rows.groupBy("track_uri").agg(
        F.min("track_name").alias("track_name"),
        F.min("artist_uri").alias("artist_uri"),
        F.min("artist_name").alias("artist_name"),
        F.min("album_uri").alias("album_uri"),
        F.min("album_name").alias("album_name"),
        F.min("duration_ms").alias("duration_ms"),
    )

    # Gold pre-index: binary implicit feedback, not explicit stars or play counts.
    # DISTINCT excludes repeat appearances of the same song within one playlist.
    interactions_uri = (
        playlist_tracks.select("playlist_id", "track_uri")
        .distinct()
        .withColumn("rating", F.lit(1.0).cast("float"))
    )
    return playlists, tracks, playlist_tracks, interactions_uri


def write_parquet(df, path: str) -> None:
    df.write.mode("overwrite").option("compression", "snappy").parquet(path)
    print(f"  Saved {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Spotify Million Playlist ETL")
    parser.add_argument(
        "--input", required=True,
        help="Local or HDFS path/glob to mpd.slice.*.json files",
    )
    parser.add_argument(
        "--output", required=True,
        help="Local output directory or HDFS URI, without trailing slash",
    )
    parser.add_argument(
        "--spark-master", default="local[4]",
        help="Spark master, e.g. local[4] for your laptop or yarn for a cluster",
    )
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument(
        "--skip-index", action="store_true",
        help="Only write the Parquet data layers; do not fit StringIndexer",
    )
    args = parser.parse_args()
    if args.shuffle_partitions < 1:
        parser.error("--shuffle-partitions must be >= 1")

    spark = (
        SparkSession.builder.appName("Spotify-MPD-ETL")
        .master(args.spark_master)
        .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
        .config("spark.driver.memory", args.driver_memory)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    base = args.output.rstrip("/")

    try:
        playlists, tracks, playlist_tracks, interactions_uri = build_tables(
            spark, args.input
        )
        print("Writing Silver tables...")
        write_parquet(playlists, f"{base}/silver/playlists")
        write_parquet(tracks, f"{base}/silver/tracks")
        write_parquet(playlist_tracks, f"{base}/silver/playlist_tracks")
        write_parquet(interactions_uri, f"{base}/gold/interactions_uri")

        if not args.skip_index:
            print("Fitting StringIndexer on unique track URIs...")
            # Training on one occurrence per playlist makes frequent songs receive
            # smaller indices, and guarantees the same mapping for all tables.
            indexer = StringIndexer(
                inputCol="track_uri",
                outputCol="track_id_double",
                handleInvalid="error",
                stringOrderType="frequencyDesc",
            )
            index_model = indexer.fit(interactions_uri)
            model_path = f"{base}/models/track_indexer"
            index_model.write().overwrite().save(model_path)
            print(f"  Saved {model_path}")

            indexed = (
                index_model.transform(interactions_uri)
                .withColumn("track_id", F.col("track_id_double").cast("int"))
            )
            write_parquet(
                indexed.select("playlist_id", "track_id", "rating"),
                f"{base}/gold/als_interactions",
            )
            write_parquet(
                indexed.select("track_uri", "track_id").distinct(),
                f"{base}/gold/track_mapping",
            )

        print("ETL finished successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
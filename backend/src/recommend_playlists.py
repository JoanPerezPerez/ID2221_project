"""Generate ALS top-K playlist continuations as Parquet (Spark-side).

Known songs are filtered. For scalability the unfiltered candidate pool is
bounded, so results can be approximate after filtering known songs.
"""
from __future__ import annotations

import argparse
import os
import sys
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession, Window, functions as F


def main():
    p = argparse.ArgumentParser(description="Batch playlist continuation from the full ALS model")
    p.add_argument("--input", required=True, help="ETL root containing gold/als_interactions")
    p.add_argument("--model", required=True, help="Path to FULL ALS model directory")
    p.add_argument("--output", required=True, help="Serving root for Parquet output")
    p.add_argument("--master", default="local[4]")
    p.add_argument("--k", type=int, default=20)
    p.add_argument("--candidate-pool", type=int, default=100)
    p.add_argument("--max-users", type=int, default=0, help="0 = ALL playlists; >0 = first N for pilot")
    p.add_argument("--shuffle-partitions", type=int, default=200)
    args = p.parse_args()
    if min(args.k, args.candidate_pool, args.shuffle_partitions) < 1 or args.max_users < 0:
        p.error("k, candidate pool, partitions > 0 and max-users >= 0")
    if args.candidate_pool < args.k:
        p.error("candidate-pool must be >= k")
    if os.name == "nt" and args.master.startswith("local"):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (SparkSession.builder.appName("MPD-Playlist-Serving")
             .master(args.master)
             .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    input_base = args.input.rstrip("/\\")
    output_base = args.output.rstrip("/\\")
    try:
        model = ALSModel.load(args.model)
        all_users = model.userFactors.select(F.col("id").cast("int").alias("playlist_id"))
        if args.max_users:
            users = all_users.orderBy("playlist_id").limit(args.max_users)
            raw = model.recommendForUserSubset(users, args.candidate_pool)
        else:
            users = all_users
            raw = model.recommendForAllUsers(args.candidate_pool)
        candidates = (raw.select("playlist_id", F.posexplode("recommendations").alias("source_rank", "rec"))
                      .select("playlist_id", "source_rank",
                              F.col("rec.track_id").cast("int").alias("track_id"),
                              F.col("rec.rating").cast("float").alias("score")))
        interactions = (spark.read.parquet(f"{input_base}/gold/als_interactions")
                        .select(F.col("playlist_id").cast("int").alias("playlist_id"),
                                F.col("track_id").cast("int").alias("track_id")))
        # Exclude every song the playlist already contains.
        if args.max_users:
            interactions = interactions.join(users, "playlist_id", "inner")
        unseen = candidates.join(interactions, ["playlist_id", "track_id"], "left_anti")
        w = Window.partitionBy("playlist_id").orderBy("source_rank")
        topk = (unseen.withColumn("rank", F.row_number().over(w))
                .filter(F.col("rank") <= args.k)
                .select("playlist_id", "rank", "track_id", "score"))
        grouped = (topk.groupBy("playlist_id")
                   .agg(F.sort_array(F.collect_list(
                       F.struct("rank", "track_id", "score"))).alias("recommendations")))
        result = (users.join(grouped, "playlist_id", "left")
                  .withColumn("recommendations", F.coalesce(
                      F.col("recommendations"),
                      F.expr("cast(array() as array<struct<rank:int,track_id:int,score:float>>)")
                  )))
        path = f"{output_base}/playlist_recommendations"
        result.write.mode("overwrite").option("compression", "snappy").parquet(path)
        print(f"Saved playlist recommendations to {path}")
        print("NOTE: if candidate-pool is small, some playlists may have fewer than K unseen songs.")
        print("The saved scores are ALS ranking scores, NOT probabilities.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

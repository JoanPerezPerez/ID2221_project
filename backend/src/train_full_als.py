"""Fit FINAL implicit ALS on 100% of the cleaned MPD playlist-track pairs.

This is deliberately distinct from train_als.py (holdout evaluation).
Run evaluation and parameter selection BEFORE fitting this production model.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, functions as F


def args_parser():
    p = argparse.ArgumentParser(description="Train final ALS on all MPD interactions")
    p.add_argument("--input", required=True, help="ETL root containing gold/als_interactions")
    p.add_argument("--output", required=True, help="Destination model folder root")
    p.add_argument("--master", default="local[4]")
    p.add_argument("--rank", type=int, default=16)
    p.add_argument("--max-iter", type=int, default=10)
    p.add_argument("--reg", type=float, default=0.1)
    p.add_argument("--alpha", type=float, default=20.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--shuffle-partitions", type=int, default=200)
    p.add_argument("--user-blocks", type=int, default=20)
    p.add_argument("--item-blocks", type=int, default=20)
    p.add_argument("--checkpoint-dir", default="", help="Optional HDFS/local checkpoint dir")
    args = p.parse_args()
    if min(args.rank, args.max_iter, args.shuffle_partitions, args.user_blocks, args.item_blocks) < 1:
        p.error("rank/iterations/partitions/blocks must be positive")
    if args.reg < 0 or args.alpha <= 0:
        p.error("reg must be >= 0 and alpha must be positive")
    return args


def main():
    args = args_parser()
    if os.name == "nt" and args.master.startswith("local"):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (SparkSession.builder.appName("MPD-Full-ALS")
             .master(args.master)
             .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    if args.checkpoint_dir:
        spark.sparkContext.setCheckpointDir(args.checkpoint_dir)

    output = args.output.rstrip("/\\")
    input_base = args.input.rstrip("/\\")
    try:
        raw = spark.read.parquet(f"{input_base}/gold/als_interactions")
        # The ETL Gold table already contains DISTINCT playlist-track pairs.
        # The ETL created an implicit binary signal: the playlist contains the song.
        data = (raw.select(F.col("playlist_id").cast("int").alias("playlist_id"),
                           F.col("track_id").cast("int").alias("track_id"))
                .dropna()
                .withColumn("rating", F.lit(1.0).cast("float")))
        n = data.count()
        if n == 0:
            raise ValueError("The ALS interactions table is empty")
        print(f"Training FULL ALS using {n:,} distinct playlist-track interactions", flush=True)
        print("No holdout: 100% of these interactions will be used.", flush=True)

        als = ALS(userCol="playlist_id", itemCol="track_id", ratingCol="rating",
                  implicitPrefs=True, alpha=args.alpha, rank=args.rank,
                  maxIter=args.max_iter, regParam=args.reg, seed=args.seed,
                  numUserBlocks=args.user_blocks, numItemBlocks=args.item_blocks,
                  coldStartStrategy="drop", intermediateStorageLevel="MEMORY_AND_DISK",
                  finalStorageLevel="MEMORY_AND_DISK")
        model = als.fit(data)
        model.write().overwrite().save(f"{output}/model")
        users = model.userFactors.count()
        items = model.itemFactors.count()
        metadata = {
            "trained_at_utc": datetime.now(timezone.utc).isoformat(),
            "training_mode": "final_full_dataset_no_holdout",
            "interactions": n, "playlists_with_factors": users,
            "tracks_with_factors": items,
            "rank": args.rank, "max_iter": args.max_iter,
            "reg": args.reg, "alpha": args.alpha, "seed": args.seed,
            "source": args.input,
            "note": "This full-data model has no unbiased test metrics. Use the separate holdout evaluation outputs.",
        }
        # JVM-only output, compatible with HDFS and avoiding Windows Python workers.
        (spark.range(1).select(*[F.lit(v).alias(k) for k, v in metadata.items()])
         .write.mode("overwrite").json(f"{output}/metadata_spark"))
        if "://" not in output:
            dest = Path(output)
            dest.mkdir(parents=True, exist_ok=True)
            (dest / "metadata.json").write_text(
                json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(metadata, indent=2, ensure_ascii=False), flush=True)
        print(f"Saved FINAL model to {output}/model", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

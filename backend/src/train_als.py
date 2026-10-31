"""Phase 4: implicit-feedback ALS for Spotify Million Playlist Dataset.

Inputs from Phase 1 ETL:
  <input>/gold/als_interactions
  <input>/gold/track_mapping
  <input>/silver/playlist_tracks    (for --split-strategy tail)
  <input>/silver/tracks

Features:
  * Playlist-level train / holdout split (tail or deterministic random).
  * Implicit ALS with pre-existing numeric playlist/track IDs.
  * Ranking evaluation with previously-seen songs removed.
  * Correct array<double> types for PySpark RankingEvaluator.
  * JVM-only Spark metrics export (avoids Python worker creation on Windows).
  * Explicit reporting of test songs not present in the training catalog.

Usage (PowerShell):
  python src/train_als.py --input data/processed --output reports/als --rank 8 --max-iter 5 --k 10 --max-eval-users 50 --exact-topk

Note: The ETL's track mapping was constructed on the complete dataset.
For strict leakage-controlled experiments, fit the track-ID mapping from
training data only. Current reporting excludes test-only items from ranking
metrics, and reports their fraction separately.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from pyspark.ml.evaluation import RankingEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame, SparkSession, Window, functions as F


def split_within_playlists(
    df: DataFrame,
    fraction: float,
    seed: int,
    ordered_first_positions: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Reserve at least one item for training in every nonempty playlist."""
    counts = df.groupBy("playlist_id").agg(F.count("*").alias("n"))

    if ordered_first_positions is None:
        # Deterministic pseudo-random holdout within each playlist.
        joined = df
        sort_columns = [
            F.xxhash64("playlist_id", "track_id", F.lit(seed)),
            F.col("track_id"),
        ]
    else:
        # Last first-occurring distinct songs are the held-out continuation.
        joined = df.join(
            ordered_first_positions, ["playlist_id", "track_id"], "left"
        ).fillna({"first_position": -1})
        sort_columns = [F.col("first_position").desc(), F.col("track_id").desc()]

    numbered = (
        joined.join(counts, "playlist_id")
        .withColumn(
            "holdout_n",
            F.when(
                F.col("n") >= 2,
                F.least(
                    F.col("n") - F.lit(1),
                    F.greatest(
                        F.lit(1),
                        F.floor(F.col("n") * F.lit(fraction)).cast("int"),
                    ),
                ),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("playlist_id").orderBy(*sort_columns)
            ),
        )
    )

    cols = ["playlist_id", "track_id", "rating"]
    test = numbered.filter(F.col("rn") <= F.col("holdout_n")).select(*cols)
    train = numbered.filter(F.col("rn") > F.col("holdout_n")).select(*cols)
    return train, test


def get_predictions(
    model,
    train: DataFrame,
    labels: DataFrame,
    pool: int,
    k: int,
) -> DataFrame:
    """Rank top-K unseen tracks per playlist and retain users with no results."""
    users = labels.select("playlist_id").distinct()
    raw = model.recommendForUserSubset(users, pool)

    candidates = (
        raw.select(
            "playlist_id",
            F.posexplode("recommendations").alias("original_rank", "rec"),
        )
        .select(
            "playlist_id",
            "original_rank",
            F.col("rec.track_id").cast("int").alias("track_id"),
        )
    )

    # Do not recommend songs that were present in the training portion.
    seen = train.join(users, "playlist_id", "inner").select(
        "playlist_id", "track_id"
    )
    unseen = candidates.join(seen, ["playlist_id", "track_id"], "left_anti")

    ranking = Window.partitionBy("playlist_id").orderBy("original_rank")
    topk = (
        unseen.withColumn("recommendation_rank", F.row_number().over(ranking))
        .filter(F.col("recommendation_rank") <= k)
    )
    ranked = (
        topk.groupBy("playlist_id")
        .agg(
            F.sort_array(
                F.collect_list(F.struct("recommendation_rank", "track_id"))
            ).alias("ordered")
        )
        .select(
            "playlist_id",
            F.expr("transform(ordered, x -> x.track_id)").alias("prediction"),
        )
    )

    return (
        labels.join(ranked, "playlist_id", "left")
        .withColumn(
            "prediction",
            F.coalesce("prediction", F.expr("cast(array() as array<int>)")),
        )
        .select("playlist_id", "prediction", "label")
    )


def save_metrics(spark: SparkSession, metrics: dict, output: str) -> None:
    """Write metrics without spark.createDataFrame(Python tuples).

    spark.range() and lit() generate rows entirely in the JVM, avoiding the
    'Python worker failed to connect back' problem experienced on Windows.
    """
    columns = [F.lit(value).alias(key) for key, value in metrics.items()]
    (
        spark.range(1)
        .select(*columns)
        .coalesce(1)
        .write.mode("overwrite")
        .json(f"{output}/metrics")
    )

    # Convenience copy as a conventional .json file for local experiments.
    # Remote paths are written via Spark only (e.g. hdfs://, s3a://).
    if not output.lower().startswith(
        ("hdfs://", "s3a://", "s3://", "abfss://", "abfs://", "gs://", "file://")
    ):
        target = Path(output)
        target.mkdir(parents=True, exist_ok=True)
        (target / "metrics.json").write_text(
            json.dumps(metrics, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MPD Phase 4: implicit ALS")
    parser.add_argument("--input", default="data/processed", help="ETL output root")
    parser.add_argument("--output", default="reports/als", help="Local or HDFS output root")
    parser.add_argument("--spark-master", default="local[4]")
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--test-fraction", type=float, default=0.2)
    parser.add_argument(
        "--split-strategy",
        choices=["tail", "random"],
        default="tail",
        help="tail: last songs in playlist; random: deterministic hashed holdout",
    )
    parser.add_argument("--rank", type=int, default=16)
    parser.add_argument("--max-iter", type=int, default=10)
    parser.add_argument("--reg", type=float, default=0.1)
    parser.add_argument("--alpha", type=float, default=20.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-eval-users", type=int, default=500)
    parser.add_argument(
        "--candidate-pool",
        type=int, default=200,
        help="Unfiltered candidate count per playlist before excluding known songs",
    )
    parser.add_argument(
        "--exact-topk",
        action="store_true",
        help="Retrieve enough candidates for the exact top-K unseen ranking",
    )

    args = parser.parse_args()
    positive = (
        args.k, args.rank, args.max_iter, args.max_eval_users,
        args.candidate_pool, args.shuffle_partitions,
    )
    if not (0 < args.test_fraction < 1) or min(positive) < 1:
        parser.error("test-fraction must be in (0,1); K/rank/iterations/users/pool/partitions must be > 0")
    if args.reg < 0 or args.alpha <= 0:
        parser.error("reg must be >= 0 and alpha must be > 0")
    return args


def main() -> None:
    args = get_args()

    # When running Spark locally on Windows, do not let Java launch the
    # Microsoft Store 'python3' alias instead of this virtual environment.
    if os.name == "nt" and args.spark_master.startswith("local"):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder
        .appName("MPD-Implicit-ALS")
        .master(args.spark_master)
        .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    base = args.input.rstrip("/\\")
    out = args.output.rstrip("/\\")

    try:
        interactions = (
            spark.read.parquet(f"{base}/gold/als_interactions")
            .select(
                F.col("playlist_id").cast("int").alias("playlist_id"),
                F.col("track_id").cast("int").alias("track_id"),
                F.col("rating").cast("float").alias("rating"),
            )
            .dropna(subset=["playlist_id", "track_id", "rating"])
            .dropDuplicates(["playlist_id", "track_id"])
        )

        ordered_positions = None
        if args.split_strategy == "tail":
            track_map = (
                spark.read.parquet(f"{base}/gold/track_mapping")
                .select("track_uri", F.col("track_id").cast("int").alias("track_id"))
            )
            ordered_positions = (
                spark.read.parquet(f"{base}/silver/playlist_tracks")
                .groupBy("playlist_id", "track_uri")
                .agg(F.min("position").alias("first_position"))
                .join(track_map, "track_uri", "inner")
                .select(
                    F.col("playlist_id").cast("int").alias("playlist_id"),
                    "track_id", "first_position",
                )
            )

        train, test = split_within_playlists(
            interactions, args.test_fraction, args.seed, ordered_positions
        )
        train = train.cache()
        test = test.cache()
        train_count = train.count()
        test_count = test.count()
        if train_count == 0 or test_count == 0:
            raise ValueError(
                "Not enough interactions for train/test. "
                "Use more playlists with at least two distinct tracks."
            )
        print(f"Train interactions: {train_count:,} | Holdout interactions: {test_count:,}")

        train_catalog = train.select("track_id").distinct().cache()
        catalog_size = train_catalog.count()
        test_known = test.join(train_catalog, "track_id", "inner").cache()
        known_count = test_known.count()
        cold_count = test_count - known_count
        if known_count == 0:
            raise ValueError(
                "All held-out tracks are unseen in training. "
                "ALS cannot recommend them. Use more overlapping playlists."
            )

        labels_all = test_known.groupBy("playlist_id").agg(
            F.collect_set("track_id").alias("label")
        )
        labels = (
            labels_all
            .orderBy(F.xxhash64("playlist_id", F.lit(args.seed)), "playlist_id")
            .limit(args.max_eval_users)
            .cache()
        )
        evaluated_playlists = labels.count()

        max_seen = (
            train.join(labels.select("playlist_id"), "playlist_id", "inner")
            .groupBy("playlist_id").count()
            .agg(F.max("count").alias("max_seen"))
            .first()["max_seen"]
        ) or 0
        required_pool = min(catalog_size, args.k + max_seen)
        pool = min(
            catalog_size,
            required_pool if args.exact_topk else max(args.candidate_pool, args.k),
        )
        exact_rankings = pool >= required_pool
        if not exact_rankings:
            print(
                f"WARNING: candidate pool {pool} is smaller than {required_pool}. "
                "Metrics use potentially truncated top-K; use --exact-topk."
            )

        als = ALS(
            userCol="playlist_id",
            itemCol="track_id",
            ratingCol="rating",
            implicitPrefs=True,
            rank=args.rank,
            maxIter=args.max_iter,
            regParam=args.reg,
            alpha=args.alpha,
            coldStartStrategy="drop",
            seed=args.seed,
            nonnegative=False,
        )

        print("Training implicit-feedback ALS...")
        model = als.fit(train)
        model.write().overwrite().save(f"{out}/model")
        train.write.mode("overwrite").parquet(f"{out}/train")
        test.write.mode("overwrite").parquet(f"{out}/test_holdout")

        # ALS recommendation IDs and label IDs remain integers for joins and
        # output. Casting is ONLY needed in the evaluator input DataFrame.
        predictions = get_predictions(model, train, labels, pool, args.k).cache()
        evaluation_input = predictions.select(
            "playlist_id",
            F.transform("prediction", lambda x: x.cast("double")).alias("prediction"),
            F.transform("label", lambda x: x.cast("double")).alias("label"),
        )
        # Both evaluator inputs are array<double>, not array<int>.
        print("RankingEvaluator schema:")
        evaluation_input.printSchema()

        recall = RankingEvaluator(
            metricName="recallAtK", k=args.k,
            predictionCol="prediction", labelCol="label",
        ).evaluate(evaluation_input)
        ndcg = RankingEvaluator(
            metricName="ndcgAtK", k=args.k,
            predictionCol="prediction", labelCol="label",
        ).evaluate(evaluation_input)

        # Save integer track IDs, not the evaluator's floating-point copies.
        predictions.write.mode("overwrite").parquet(f"{out}/eval_predictions")
        track_mapping = (
            spark.read.parquet(f"{base}/gold/track_mapping")
            .select("track_uri", F.col("track_id").cast("int").alias("track_id"))
        )
        track_metadata = (
            spark.read.parquet(f"{base}/silver/tracks")
            .select("track_uri", "track_name", "artist_name")
            .dropDuplicates(["track_uri"])
        )
        demonstration = (
            predictions.select(
                "playlist_id",
                F.posexplode("prediction").alias("position_zero_based", "track_id"),
            )
            .join(track_mapping, "track_id", "left")
            .join(track_metadata, "track_uri", "left")
            .select(
                "playlist_id", "position_zero_based", "track_id",
                "track_uri", "track_name", "artist_name",
            )
        )
        demonstration.write.mode("overwrite").parquet(
            f"{out}/sample_recommendations"
        )

        metrics = {
            "recall_at_k": float(recall),
            "ndcg_at_k": float(ndcg),
            "k": args.k,
            "rank": args.rank,
            "max_iter": args.max_iter,
            "regularization": args.reg,
            "alpha": args.alpha,
            "seed": args.seed,
            "test_fraction": args.test_fraction,
            "split_strategy": args.split_strategy,
            "train_interactions": train_count,
            "test_interactions": test_count,
            "known_test_interactions": known_count,
            "cold_start_test_interactions": cold_count,
            "cold_start_fraction": cold_count / test_count,
            "evaluated_playlists": evaluated_playlists,
            "train_catalog_items": catalog_size,
            "candidate_pool": pool,
            "sufficient_pool_for_exact_top_k_unseen": exact_rankings,
            "note": (
                "Recall/NDCG are calculated on held-out tracks that also appear "
                "in the training catalog, for a subset of evaluable playlists. "
                "Songs known in each playlist's training portion are excluded "
                "from its recommendations. The ETL track index was created "
                "before the split; for strict isolation use a train-only mapping."
            ),
        }

        print("\n=== Phase 4: evaluation ===")
        print(json.dumps(metrics, ensure_ascii=False, indent=2))
        predictions.show(10, truncate=80)
        save_metrics(spark, metrics, out)
        print(f"Model and evaluation saved at: {out}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

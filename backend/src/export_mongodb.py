"""Stream ETL catalog and precomputed recommendations into local/Atlas MongoDB.

Reads Spark DataFrames one partition at a time with toLocalIterator(); avoids
collecting millions of documents into Python memory or requiring a Mongo Spark
connector. Uses idempotent batched ReplaceOne upserts and Mongo _id indexes.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from pymongo import MongoClient, ReplaceOne, ASCENDING


def upsert_batches(collection, documents, batch_size):
    pending = []
    count = 0
    for document in documents:
        if document.get("_id") is None:
            raise ValueError(f"Missing _id in {collection.name} document")
        pending.append(ReplaceOne({"_id": document["_id"]}, document, upsert=True))
        if len(pending) >= batch_size:
            collection.bulk_write(pending, ordered=False)
            count += len(pending)
            pending.clear()
            print(f"{collection.name}: {count:,} upserted", flush=True)
    if pending:
        collection.bulk_write(pending, ordered=False)
        count += len(pending)
    print(f"DONE {collection.name}: {count:,} records upserted", flush=True)
    return count


def track_documents(spark, base):
    from pyspark.sql import functions as F
    mapping = (spark.read.parquet(f"{base}/gold/track_mapping")
               .select(F.col("track_id").cast("int").alias("track_id"), "track_uri"))
    tracks = (spark.read.parquet(f"{base}/silver/tracks")
              .select("track_uri", "track_name", "artist_name", "album_name", "duration_ms")
              .dropDuplicates(["track_uri"]))
    for row in mapping.join(tracks, "track_uri", "left").toLocalIterator():
        d = row.asDict(recursive=True)
        d["_id"] = int(d.pop("track_id"))
        yield d


def playlist_documents(spark, base):
    df = (spark.read.parquet(f"{base}/silver/playlists")
          .select("playlist_id", "name", "num_tracks", "num_followers"))
    for row in df.toLocalIterator():
        d = row.asDict(recursive=True)
        d["_id"] = int(d.pop("playlist_id"))
        yield d


def playlist_recommendation_documents(spark, parquet_path):
    for row in spark.read.parquet(parquet_path).toLocalIterator():
        d = row.asDict(recursive=True)
        d["_id"] = int(d.pop("playlist_id"))
        d["recommendations"] = [
            {"rank": int(r["rank"]), "track_id": int(r["track_id"]),
             "score": float(r["score"])}
            for r in (d["recommendations"] or [])
        ]
        yield d


def track_recommendation_documents(directory):
    folder = Path(directory)
    files = sorted(folder.glob("part-*.jsonl"))
    if not files:
        raise FileNotFoundError(f"No part-*.jsonl in {folder}")
    for file in files:
        with file.open("r", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                data = json.loads(line)
                yield {"_id": int(data["track_id"]),
                       "recommendations": [
                           {"rank": int(r["rank"]), "track_id": int(r["track_id"]),
                            "similarity": float(r["similarity"])}
                           for r in data["recommendations"]]}


def main():
    p = argparse.ArgumentParser(description="Import precomputed MPD recommendations into MongoDB")
    p.add_argument("--input", required=True, help="ETL root")
    p.add_argument("--playlist-recs", default="reports/serving/playlist_recommendations")
    p.add_argument("--track-recs", default="reports/serving/track_similar_jsonl")
    p.add_argument("--db", default="mpd_recommender")
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--master", default="local[4]")
    p.add_argument("--only", choices=["tracks", "playlists", "playlist_recommendations", "track_recommendations"],
                   action="append", help="Run one or several collection imports; default imports all four")
    args = p.parse_args()
    if args.batch_size < 1:
        p.error("batch-size must be >= 1")
    tasks = list(dict.fromkeys(args.only or [
        "tracks", "playlists", "playlist_recommendations", "track_recommendations"]))
    url = os.environ.get("MONGODB_URI", "mongodb://127.0.0.1:27017")
    client = MongoClient(url, serverSelectionTimeoutMS=10000, retryWrites=True)
    client.admin.command("ping")
    db = client[args.db]
    print(f"Connected to MongoDB database '{args.db}'", flush=True)
    db["tracks"].create_index([("track_uri", ASCENDING)], unique=True)
    # Every collection also has a unique MongoDB _id index automatically.

    need_spark = any(x != "track_recommendations" for x in tasks)
    spark = None
    if need_spark:
        if os.name == "nt" and args.master.startswith("local"):
            os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
            os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("MPD-MongoDB-Export").master(args.master).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
    base = args.input.rstrip("/\\")
    try:
        if "tracks" in tasks:
            upsert_batches(db["tracks"], track_documents(spark, base), args.batch_size)
        if "playlists" in tasks:
            upsert_batches(db["playlists"], playlist_documents(spark, base), args.batch_size)
        if "playlist_recommendations" in tasks:
            upsert_batches(db["playlist_recommendations"],
                           playlist_recommendation_documents(spark, args.playlist_recs), args.batch_size)
        if "track_recommendations" in tasks:
            upsert_batches(db["track_recommendations"],
                           track_recommendation_documents(args.track_recs), args.batch_size)
        for collection in tasks:
            print(f"{collection}: {db[collection].estimated_document_count():,} estimated documents")
    finally:
        if spark is not None:
            spark.stop()
        client.close()


if __name__ == "__main__":
    main()

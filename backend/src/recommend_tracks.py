"""Approximate top-K cosine neighbours for ALS item factors using FAISS HNSW.

All trained track vectors are indexed. By default only the N most frequent
tracks are used as queries, because 2M+ queries is an expensive batch job.
The output is local newline-delimited JSON, streamed to disk in small shards.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description="Approximate cosine neighbours from ALS track factors")
    p.add_argument("--model", required=True, help="Saved FULL ALS model directory")
    p.add_argument("--input", required=True, help="ETL root, for popularity selection")
    p.add_argument("--output", required=True, help="Local output directory for *.jsonl shards")
    p.add_argument("--master", default="local[4]")
    p.add_argument("--k", type=int, default=20)
    p.add_argument("--max-query-tracks", type=int, default=1000,
                   help="Number of popular songs to produce recommendations for")
    p.add_argument("--all-tracks", action="store_true", help="Query every track (expensive)")
    p.add_argument("--m", type=int, default=24, help="HNSW neighbors per node")
    p.add_argument("--ef-construction", type=int, default=120)
    p.add_argument("--ef-search", type=int, default=96)
    p.add_argument("--batch-size", type=int, default=256)
    p.add_argument("--per-file", type=int, default=10000)
    p.add_argument("--threads", type=int, default=4)
    args = p.parse_args()
    if min(args.k, args.max_query_tracks, args.m, args.ef_construction,
           args.ef_search, args.batch_size, args.per_file, args.threads) < 1:
        p.error("All numeric options must be positive")

    try:
        import numpy as np
        import faiss
        from pyspark.ml.recommendation import ALSModel
        from pyspark.sql import SparkSession, functions as F
    except ImportError as exc:
        raise SystemExit("Requires pyspark, numpy, faiss-cpu. Install with: python -m pip install -r requirements-serving.txt") from exc

    if os.name == "nt" and args.master.startswith("local"):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = SparkSession.builder.appName("MPD-Cosine-HNSW").master(args.master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    output_dir = Path(args.output)
    if "://" in args.output:
        raise ValueError("FAISS JSONL output must be a local directory; copy it to MongoDB host later")
    output_dir.mkdir(parents=True, exist_ok=True)
    # Avoid mixing stale shards from an earlier run with this run.
    existing = list(output_dir.glob("part-*.jsonl"))
    if existing:
        raise ValueError(f"Output has {len(existing)} existing JSONL shards. Use an empty folder: {output_dir}")

    try:
        model = ALSModel.load(args.model)
        factors = model.itemFactors.select(F.col("id").cast("int").alias("id"), "features")
        n = factors.count()
        if n <= 1:
            raise ValueError("Need at least two track factor vectors")
        first = factors.first()
        dim = len(first.features)
        print(f"Loading ALL {n:,} factor vectors (dim={dim}) into local FAISS index")
        print("Ensure enough RAM: for millions of tracks HNSW may need multiple GB.")
        # Fixed-size NumPy arrays avoid collecting millions of Python Row objects.
        ids = np.empty(n, dtype=np.int32)
        vectors = np.empty((n, dim), dtype=np.float32)
        for i, row in enumerate(factors.toLocalIterator()):
            ids[i] = int(row.id)
            vectors[i] = np.asarray(row.features, dtype=np.float32)
        # Normalizing factors turns inner product into cosine similarity.
        faiss.normalize_L2(vectors)
        faiss.omp_set_num_threads(args.threads)
        index = faiss.IndexHNSWFlat(dim, args.m, faiss.METRIC_INNER_PRODUCT)
        index.hnsw.efConstruction = args.ef_construction
        index.hnsw.efSearch = max(args.ef_search, args.k + 1)
        print("Building HNSW index (approximate nearest neighbours)...", flush=True)
        index.add(vectors)

        if args.all_tracks:
            source_positions = np.arange(n, dtype=np.int64)
        else:
            # Only the QUERIES are limited: candidates still cover ALL factor vectors.
            # Songs are ranked by number of playlists containing them.
            interactions_path = args.input.rstrip("/\\") + "/gold/als_interactions"
            query_ids = [int(r.track_id) for r in (
                spark.read.parquet(interactions_path)
                .groupBy("track_id").count()
                .orderBy(F.desc("count"), F.asc("track_id"))
                .limit(args.max_query_tracks)
                .select("track_id").collect()
            )]
            sorted_pos = np.argsort(ids)
            sorted_ids = ids[sorted_pos]
            query_ids_arr = np.asarray(query_ids, dtype=np.int32)
            at = np.searchsorted(sorted_ids, query_ids_arr)
            valid = at < n
            positions = np.full(len(at), -1, dtype=np.int64)
            positions[valid] = sorted_pos[at[valid]]
            valid = valid & (ids[np.maximum(positions, 0)] == query_ids_arr)
            source_positions = positions[valid]
        print(f"Query tracks: {len(source_positions):,}; catalog indexed: {n:,}")

        total = 0
        shard = -1
        handle = None
        try:
            for start in range(0, len(source_positions), args.batch_size):
                positions = source_positions[start:start + args.batch_size]
                distances, neighbors = index.search(
                    np.ascontiguousarray(vectors[positions]), min(n, args.k + 8))
                for row_no, pos in enumerate(positions):
                    if total % args.per_file == 0:
                        if handle is not None:
                            handle.close()
                        shard += 1
                        handle = (output_dir / f"part-{shard:05d}.jsonl").open("w", encoding="utf-8")
                    source_id = int(ids[pos])
                    recs = []
                    for internal_id, similarity in zip(neighbors[row_no], distances[row_no]):
                        if internal_id < 0:
                            continue
                        neighbor_id = int(ids[internal_id])
                        if neighbor_id == source_id:
                            continue
                        recs.append({"rank": len(recs) + 1, "track_id": neighbor_id,
                                     "similarity": float(similarity)})
                        if len(recs) >= args.k:
                            break
                    handle.write(json.dumps({"track_id": source_id, "recommendations": recs},
                                            ensure_ascii=False) + "\n")
                    total += 1
                if (start // args.batch_size) % 100 == 0:
                    print(f"Processed {min(start + args.batch_size, len(source_positions)):,} / {len(source_positions):,}")
        finally:
            if handle is not None:
                handle.close()
        summary = {"indexed_tracks": n, "query_tracks_exported": total, "k": args.k,
                   "method": "cosine_similarity_unit_normalized_ALS_factors_FAISS_HNSW_approximate",
                   "all_tracks": args.all_tracks}
        (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

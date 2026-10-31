"""Generate a larger SYNTHETIC MPD-like JSON file for local end-to-end tests.

Synthetic identifiers are NOT real Spotify tracks. Stdlib-only; no Spark required.
"""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Create synthetic MPD fixture")
    parser.add_argument("--output", default="data/sample/mpd.generated.json")
    parser.add_argument("--playlists", type=int, default=120)
    parser.add_argument("--tracks", type=int, default=90)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.playlists < 3 or args.tracks < 10:
        parser.error("Use at least 3 playlists and 10 tracks")

    rng = random.Random(args.seed)
    result = []
    common = list(range(min(8, args.tracks)))
    genres = 4
    for pid in range(args.playlists):
        segment = pid % genres
        pool = [i for i in range(len(common), args.tracks) if i % genres == segment]
        # Popular tracks co-occur with more specialized genre tracks.
        selected = rng.sample(common, k=min(len(common), 4))
        selected += rng.sample(pool, k=min(len(pool), rng.randint(8, min(15, max(8, len(pool))))))
        rng.shuffle(selected)
        rows = []
        for pos, tid in enumerate(selected):
            artist_id = tid % 18
            rows.append({
                "pos": pos,
                "track_uri": f"spotify:track:synthetic{tid:05d}",
                "track_name": f"Synthetic song {tid}",
                "artist_uri": f"spotify:artist:synthetic{artist_id:03d}",
                "artist_name": f"Synthetic artist {artist_id}",
                "album_uri": f"spotify:album:synthetic{tid // 8:03d}",
                "album_name": f"Synthetic album {tid // 8}",
                "duration_ms": 150000 + (tid % 12) * 11000,
            })
        result.append({
            "pid": 1_000_000 + pid,
            "name": f"Synthetic playlist {pid}",
            "num_tracks": len(rows),
            "num_followers": rng.randint(0, 50),
            "modified_at": 1_500_000_000 + pid,
            "tracks": rows,
        })
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"info": {"description": "Synthetic: not Spotify data"},
                               "playlists": result}, ensure_ascii=False), encoding="utf-8")
    print(f"Created {path} with {len(result)} synthetic playlists.")


if __name__ == "__main__":
    main()

"""Optional local plot generation from the Spark CSV reports; install matplotlib.
Run only on the local machine after `eda.py` writes reports/eda.
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path


def read_spark_csv(folder):
    files = sorted(Path(folder).glob("part-*.csv"))
    if not files:
        raise FileNotFoundError(f"No Spark part-*.csv in {folder}. Run eda.py first.")
    with files[0].open(encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-input", default="reports/eda")
    parser.add_argument("--figures", default="reports/figures")
    args = parser.parse_args()
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise SystemExit("Install matplotlib: python -m pip install matplotlib") from exc
    destination = Path(args.figures)
    destination.mkdir(parents=True, exist_ok=True)

    hist = read_spark_csv(Path(args.report_input) / "playlist_size_buckets")
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar([r["bucket"].split(": ", 1)[-1] for r in hist],
           [int(r["playlist_count"]) for r in hist])
    ax.set(xlabel="Distinct songs per playlist", ylabel="Number of playlists",
           title="Spotify MPD: playlist-size distribution")
    fig.tight_layout()
    fig.savefig(destination / "playlist_sizes.png", dpi=170)
    plt.close(fig)

    top = read_spark_csv(Path(args.report_input) / "top_tracks")[:15]
    top.reverse()
    labels = [f"{r.get('track_name', '')[:24]} — {r.get('artist_name', '')[:20]}" for r in top]
    fig, ax = plt.subplots(figsize=(11, 7))
    ax.barh(labels, [int(r["playlist_count"]) for r in top])
    ax.set(xlabel="Number of playlists containing song", title="Most common songs")
    fig.tight_layout()
    fig.savefig(destination / "most_frequent_tracks.png", dpi=170)
    plt.close(fig)
    print(f"Saved plots in {destination}")


if __name__ == "__main__":
    main()

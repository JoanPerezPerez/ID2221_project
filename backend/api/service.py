"""MongoDB read queries and response shaping. Never loads the Spark model.

Mongo schemas created by src/export_mongodb.py:
tracks: {_id: int track_id, track_uri, track_name, artist_name, album_name, ...}
playlists: {_id: int playlist_id, name, ...}
track_recommendations: {_id: int track_id, recommendations: [{track_id, rank, similarity}]}
playlist_recommendations: {_id: int playlist_id, recommendations: [{track_id, rank, score}]}
"""
from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

TRACK_FIELDS = {"_id": 1, "track_uri": 1, "track_name": 1, "artist_name": 1, "album_name": 1}
TRACK_URI_PATTERN = re.compile(r"^spotify:track:[A-Za-z0-9]+$")


def public_track(document: dict[str, Any]) -> dict[str, str]:
    return {
        "track_uri": document.get("track_uri") or "",
        "track_name": document.get("track_name") or "",
        "artist_name": document.get("artist_name") or "",
        "album_name": document.get("album_name") or "",
    }


def unique_track_uris(raw: str, max_tracks: int = 50) -> list[str]:
    if not raw.strip():
        return []
    result = list(dict.fromkeys(uri.strip() for uri in raw.split(",") if uri.strip()))
    if len(result) > max_tracks:
        raise ValueError(f"Select at most {max_tracks} tracks")
    for uri in result:
        if not TRACK_URI_PATTERN.fullmatch(uri):
            raise ValueError(f"Invalid Spotify track URI: {uri[:100]}")
    return result


def search_tracks(db: Any, q: str, limit: int = 20) -> list[dict[str, str]]:
    query = q.strip()
    if not query:
        return []
    # Matches Svelte's original case-insensitive substring semantics.
    # Warning: unanchored case-insensitive regex may scan the 1.9M-track catalog.
    docs = db.tracks.find(
        {"track_name": {"$regex": re.escape(query), "$options": "i"}},
        TRACK_FIELDS,
    ).limit(limit)
    return [public_track(d) for d in docs if d.get("track_uri")]


def combined_track_recommendations(db: Any, uris: Iterable[str], k: int = 20) -> list[dict[str, Any]]:
    selected_uris = list(dict.fromkeys(uris))
    if not selected_uris:
        return []
    # Catalog stores URI and numeric _id; recommendation documents are keyed by numeric _id.
    origins = list(db.tracks.find({"track_uri": {"$in": selected_uris}}, {"_id": 1, "track_uri": 1}))
    origin_id_to_uri = {int(d["_id"]): d["track_uri"] for d in origins}
    origin_ids = set(origin_id_to_uri)
    if not origin_ids:
        return []
    # Preserve the supplied URI order when summing recommendations. De-duplicate input URIs.
    rec_docs = db.track_recommendations.find(
        {"_id": {"$in": list(origin_ids)}}, {"_id": 1, "recommendations": 1}
    )
    by_source = {int(d["_id"]): d.get("recommendations", []) for d in rec_docs}
    scores: dict[int, dict[str, float]] = {}
    for uri in selected_uris:
        source_id = next((ident for ident, original_uri in origin_id_to_uri.items() if original_uri == uri), None)
        if source_id is None:
            continue  # Missing track in our catalog.
        for neighbor in by_source.get(source_id, []):
            neighbor_id = int(neighbor["track_id"])
            if neighbor_id in origin_ids:
                continue
            similarity = float(neighbor["similarity"])
            if neighbor_id not in scores:
                scores[neighbor_id] = {"similarity": similarity, "totalScore": similarity}
            else:
                # Original mock retains the first similarity and accumulates totalScore.
                scores[neighbor_id]["totalScore"] += similarity
    if not scores:
        return []

    ranked_ids = sorted(scores, key=lambda ident: (-scores[ident]["totalScore"], ident))
    # Hydrate only the ranking candidates, not the full song catalog.
    candidates = list(db.tracks.find({"_id": {"$in": ranked_ids}}, TRACK_FIELDS))
    tracks_by_id = {int(t["_id"]): t for t in candidates if t.get("track_uri")}
    response: list[dict[str, Any]] = []
    for track_id in ranked_ids:
        track = tracks_by_id.get(track_id)
        if track is None or track["track_uri"] in selected_uris:
            continue
        response.append({
            **public_track(track),
            "similarity": scores[track_id]["similarity"],
            "totalScore": scores[track_id]["totalScore"],
        })
        if len(response) >= k:
            break
    return response


def search_playlists(db: Any, q: str, limit: int = 20) -> list[dict[str, Any]]:
    query = q.strip()
    if not query:
        return []
    return [
        {
            "playlist_id": int(d["_id"]),
            "name": d.get("name") or "",
            "num_tracks": int(d.get("num_tracks") or 0),
            "num_followers": int(d.get("num_followers") or 0),
        }
        for d in db.playlists.find(
            {"name": {"$regex": re.escape(query), "$options": "i"}},
            {"_id": 1, "name": 1, "num_tracks": 1, "num_followers": 1},
        ).limit(limit)
    ]


def playlist_recommendations(db: Any, playlist_id: int, k: int = 20) -> dict[str, Any] | None:
    playlist = db.playlists.find_one({"_id": playlist_id}, {"_id": 1, "name": 1, "num_tracks": 1})
    if not playlist:
        return None
    rec_doc = db.playlist_recommendations.find_one({"_id": playlist_id}, {"recommendations": 1})
    ranked = sorted((rec_doc or {}).get("recommendations", []), key=lambda r: int(r["rank"]))[:k]
    ids = list(dict.fromkeys(int(r["track_id"]) for r in ranked))
    catalog = list(db.tracks.find({"_id": {"$in": ids}}, TRACK_FIELDS)) if ids else []
    catalog_by_id = {int(d["_id"]): d for d in catalog}
    recs = []
    for r in ranked:
        track = catalog_by_id.get(int(r["track_id"]))
        if track is not None and track.get("track_uri"):
            recs.append({**public_track(track), "rank": int(r["rank"]), "score": float(r["score"])})
    return {
        "playlist_id": playlist_id,
        "name": playlist.get("name") or "",
        "num_tracks": int(playlist.get("num_tracks") or 0),
        "recommendations": recs,
    }

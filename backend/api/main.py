"""FastAPI serving layer for MongoDB-backed Spotify recommendations.

All handlers use ordinary `def` because PyMongo is synchronous. FastAPI runs
these functions in a threadpool, avoiding blocking the async event loop.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field

from api.service import (
    combined_track_recommendations,
    playlist_recommendations,
    search_playlists,
    search_tracks,
    unique_track_uris,
)


class Track(BaseModel):
    track_uri: str
    track_name: str
    artist_name: str
    album_name: str


class RecommendedTrack(Track):
    similarity: float
    totalScore: float


class Playlist(BaseModel):
    playlist_id: int
    name: str
    num_tracks: int
    num_followers: int


class PlaylistTrack(Track):
    rank: int
    score: float


class PlaylistRecommendations(BaseModel):
    playlist_id: int
    name: str
    num_tracks: int
    recommendations: list[PlaylistTrack] = Field(default_factory=list)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Import lazily so pure API contract tests can inject a fake DB.
    from pymongo import MongoClient

    client = MongoClient(
        os.getenv("MONGODB_URI", "mongodb://127.0.0.1:27017"),
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
    )
    client.admin.command("ping")
    app.state.mongo_client = client
    app.state.mongo_db = client[os.getenv("MONGODB_DB", "mpd_recommender_real_v1")]
    try:
        yield
    finally:
        client.close()


app = FastAPI(
    title="Spotify MPD Recommender API",
    version="1.0.0",
    description="Precomputed ALS and FAISS recommendations served from MongoDB.",
    lifespan=lifespan,
)


def get_db(request: Request) -> Any:
    return request.app.state.mongo_db


Db = Annotated[Any, Depends(get_db)]


@app.get("/api/health")
def health(db: Db) -> dict[str, str]:
    db.command("ping")
    return {"status": "ok", "database": db.name}


@app.get("/api/tracks/search", response_model=list[Track])
def tracks_search(
    db: Db,
    q: Annotated[str, Query(max_length=120)] = "",
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
) -> list[dict[str, str]]:
    return search_tracks(db, q, limit)


@app.get("/api/recommendations/tracks", response_model=list[RecommendedTrack])
def tracks_recommendations(
    db: Db,
    uris: Annotated[str, Query(max_length=4096)] = "",
    k: Annotated[int, Query(ge=1, le=20)] = 20,
) -> list[dict[str, Any]]:
    try:
        parsed = unique_track_uris(uris)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return combined_track_recommendations(db, parsed, k)


@app.get("/api/playlists/search", response_model=list[Playlist])
def playlists_search(
    db: Db,
    q: Annotated[str, Query(max_length=120)] = "",
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
) -> list[dict[str, Any]]:
    return search_playlists(db, q, limit)


@app.get("/api/recommendations/playlists/{playlist_id}", response_model=PlaylistRecommendations)
def playlist_recommendations_endpoint(
    playlist_id: int,
    db: Db,
    k: Annotated[int, Query(ge=1, le=20)] = 20,
) -> dict[str, Any]:
    result = playlist_recommendations(db, playlist_id, k)
    if result is None:
        raise HTTPException(status_code=404, detail="Playlist not found")
    return result

"""Inspect the MongoDB collection contents without a web API."""
import os
from pprint import pprint
from pymongo import MongoClient

client = MongoClient(os.getenv("MONGODB_URI", "mongodb://127.0.0.1:27017"),
                     serverSelectionTimeoutMS=10000)
db = client[os.getenv("MONGODB_DB", "mpd_recommender")]
client.admin.command("ping")
for name in ("tracks", "playlists", "playlist_recommendations", "track_recommendations"):
    collection = db[name]
    print(f"\n{name}: {collection.estimated_document_count():,} documents")
    doc = collection.find_one()
    if doc is not None:
        if "recommendations" in doc:
            doc["recommendations"] = doc["recommendations"][:3]
        pprint(doc)
client.close()

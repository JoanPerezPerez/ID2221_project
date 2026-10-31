# ID2221_project:
# Spotify Million Playlist Dataset — Music Recommendation System

An end-to-end music recommender built for the **Spotify Million Playlist Dataset (MPD)**. The project transforms raw playlist JSON into Parquet tables using **PySpark**, trains an **implicit-feedback ALS** model, computes playlist continuations and approximate track-to-track recommendations with **FAISS HNSW**, stores the results in **MongoDB**, and serves them through **FastAPI** to a **SvelteKit** frontend.

This README describes the complete local workflow on **Windows / PowerShell**, starting with the original `mpd.slice.*.json` files. All commands assume that they are executed from the indicated project directory.

## 1. Architecture

```text
Spotify MPD JSON files (backend/data/raw/)
                |
                v
       PySpark ETL (src/etl.py)
                |
     +----------+-----------+
     |                      |
Silver Parquet          Gold Parquet
(playlists, tracks,     (ALS interactions,
 playlist_tracks)        track URI/ID mapping)
     |                      |
     |                      v
     |           Implicit ALS training
     |              (full dataset)
     |                      |
     |            Saved ALS factors
     |                      |
     |       +--------------+--------------+
     |       |                             |
     |  Playlist top-K              Track cosine top-K
     |  (PySpark ALS)               (FAISS HNSW)
     |       |                             |
     |    Parquet                        JSONL
     +-------+--------------+--------------+
                            |
                            v
                    MongoDB collections
                            |
                            v
                     FastAPI REST API
                            |
                            v
                  SvelteKit API proxy
                            |
                            v
                      Web interface
```

**Important:** The API does **not** load Spark or run ALS on every request. Recommendations are calculated offline and served from MongoDB. Searchable tracks and tracks with precomputed recommendations are not necessarily the same set.

## 2. Project structure

```text
ID2221_project/
├── backend/
│   ├── .venv/                         # Python virtual environment (local)
│   ├── api/
│   │   ├── __init__.py
│   │   ├── main.py                    # FastAPI routes and response models
│   │   └── service.py                 # MongoDB queries and recommendation aggregation
│   ├── src/
│   │   ├── etl.py                     # Raw MPD JSON -> Parquet
│   │   ├── train_als.py               # Optional holdout evaluation
│   │   ├── train_full_als.py          # Train final model on all interactions
│   │   ├── recommend_playlists.py     # ALS playlist recommendations
│   │   ├── recommend_tracks.py        # FAISS track similarity
│   │   ├── export_mongodb.py          # Export catalogs and recommendations
│   │   └── check_mongodb.py          # Inspect imported MongoDB collections
│   ├── data/
│   │   ├── raw/                       # Place original mpd.slice.*.json files here
│   │   └── processed_real_v1/         # ETL output; generated
│   ├── reports/
│   │   ├── als_real_v1/               # Saved full ALS model; generated
│   │   └── serving_real_v1/           # Generated recommendations
│   ├── requirements.txt               # PySpark dependency, if present
│   ├── requirements-serving.txt       # FAISS, NumPy, PyMongo, if present
│   └── requirements-api.txt           # FastAPI, Uvicorn, PyMongo, if present
└── frontend/
    ├── src/
    │   ├── lib/
    │   └── routes/                     # Svelte pages and API proxy handlers
    ├── package.json
    └── .env                           # Server-side FastAPI URL, if used
```

The Python scripts and `api` module above must already be present in your backend. If some scripts are absent, copy them from the corresponding project modules **before running the commands**. Do not overwrite your existing frontend components when copying API proxy handlers.

## 3. Prerequisites

- **Windows 10/11** with **PowerShell** and VS Code (or another terminal/editor).
- **Python 3** and a compatible **Java JDK** for your installed PySpark version.
- **Apache Spark / PySpark** installed through pip; these project scripts use the PySpark MLlib ALS API.
- For native Windows Spark, compatible **Hadoop native Windows binaries** (`winutils.exe`, `hadoop.dll`) may be required. Match them to the Hadoop libraries bundled with your PySpark installation.
- **MongoDB** running locally (Docker Desktop + `mongo:8` is used below), or a separately configured MongoDB server.
- **Node.js >= 22.17.0** for the SvelteKit version used in this project, and npm.
- Adequate **RAM and disk space**. The full MPD contains a very large number of playlist–track interactions; FAISS HNSW and ALS training can exceed the resources of a typical laptop.

The original MPD consists of up to **1,000 JSON slices** (`mpd.slice.0-999.json`, etc.), each containing up to 1,000 playlists. The precise output counts depend on which slices are available and your ETL filters. **Do not confuse `data/sample` with the original dataset under `data/raw`.**

## 4. Backend setup (PowerShell)

Open a terminal in `ID2221_project/backend`:

```powershell
cd "C:\Users\JoanAndroidStudio\Desktop\ID2221_project\backend"

# Create the environment only if .venv does not already exist.
if (-not (Test-Path ".venv")) { py -m venv .venv }
.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip

# If your backend contains the requirements files:
python -m pip install -r requirements.txt
python -m pip install -r requirements-serving.txt
python -m pip install -r requirements-api.txt
```

If your checkout does **not** include these three requirements files, install the required packages directly instead:

```powershell
python -m pip install "pyspark==4.2.0" "numpy>=1.26" "faiss-cpu>=1.13" "pymongo>=4.9,<5" "fastapi>=0.115,<1" "uvicorn[standard]>=0.30,<1"
```

Set the Python executable used by Spark's driver and workers. These environment variables apply to the **current PowerShell session**:

```powershell
$pythonExe = (Resolve-Path ".\.venv\Scripts\python.exe").Path
$env:PYSPARK_PYTHON = $pythonExe
$env:PYSPARK_DRIVER_PYTHON = $pythonExe
$env:PYSPARK_SUBMIT_ARGS = "--driver-memory 8g pyspark-shell"

# Use these same paths in every subsequent step:
$base = "data/processed_real_v1"
$modelRoot = "reports/als_real_v1"
$serving = "reports/serving_real_v1"
$db = "mpd_recommender_real_v1"
```

`8g` is an example driver memory allocation, not a minimum or a guarantee that the complete dataset will fit. Adjust it to the physical memory available. Keep the dataset path, model directory, serving directory, and MongoDB database consistent across every command.

### Native Windows Hadoop configuration (only if needed)

If you already fixed Hadoop on your PC, **do not reinstall it**. Otherwise, check your configured binaries:

```powershell
$env:HADOOP_HOME
Test-Path "$env:HADOOP_HOME\bin\winutils.exe"
Test-Path "$env:HADOOP_HOME\bin\hadoop.dll"
```

After obtaining **compatible, trusted** binaries, configure their folder, for example:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
```

`UnsatisfiedLinkError: NativeIO$Windows.access0` generally indicates missing/incompatible native Hadoop integration. A `WARN NativeCodeLoader` line alone does not necessarily mean the job failed; inspect the final exception before troubleshooting.

## 5. Step 1 — Read the real MPD files and create Parquet tables

Put the extracted JSON files directly in `backend/data/raw/`, not `backend/data/sample/`.

Check the input:

```powershell
Test-Path "data/raw"
(Get-ChildItem "data/raw" -File -Filter "mpd.slice.*.json").Count
Get-ChildItem "data/raw" -File -Filter "mpd.slice.*.json" |
    Select-Object -First 5 -ExpandProperty Name
```

**Run the ETL** (this example reads all compatible JSON files in the `data/raw` directory):

```powershell
python src/etl.py --input "data/raw" --output "$base" --shuffle-partitions 64 --driver-memory 8g
```

If you need to use a filename pattern instead, the ETL also accepts `--input "data/raw/mpd.slice.*.json"`. However, local Hadoop glob expansion can be problematic with an incomplete Windows native-Hadoop installation; using the directory avoids that specific glob pattern. The directory should contain only the intended MPD inputs.

The ETL:

1. Reads nested multi-line MPD JSON and flattens playlists/tracks.
2. Deduplicates catalog records and writes **Silver** Parquet tables.
3. Creates distinct playlist–track interactions with implicit `rating = 1.0`.
4. Fits a `StringIndexer` to map `spotify:track:...` URIs to numeric `track_id` identifiers.
5. Writes **Gold** Parquet tables used by ALS and MongoDB export.

Expected generated tables:

| Layer | Location under `$base` | Purpose |
| --- | --- | --- |
| Silver | `silver/playlists` | Playlist metadata |
| Silver | `silver/tracks` | Unique songs and metadata |
| Silver | `silver/playlist_tracks` | Original playlist–track occurrences and positions |
| Gold | `gold/interactions_uri` | Distinct playlist/Spotify URI pairs |
| Gold | `gold/als_interactions` | Numeric `playlist_id`, `track_id`, `rating` |
| Gold | `gold/track_mapping` | Numeric `track_id` to Spotify URI mapping |
| Model mapping | `models/track_indexer` | Saved Spark StringIndexer mapping |

Verify that the essential tables exist:

```powershell
Test-Path "$base/silver/tracks"
Test-Path "$base/silver/playlists"
Test-Path "$base/gold/als_interactions"
Test-Path "$base/gold/track_mapping"
```

All four checks must return `True` before proceeding. The ETL uses **overwrite mode** for its outputs: keep the same model and recommendation artifacts synchronized with the resulting ID mapping. For a completely new experiment, use new output directories and a new MongoDB database.

## 6. Step 2 — Optional ALS evaluation (holdout)

The evaluation script measures how well ALS recovers tracks withheld from each playlist. It is **separate** from the final model used for serving.

```powershell
python src/train_als.py --input "$base" --output "reports/als_evaluation_real_v1" --rank 16 --max-iter 10 --alpha 20 --reg 0.1 --k 20 --test-fraction 0.2 --split-strategy tail --max-eval-users 500 --candidate-pool 200
```

Review the generated evaluation metrics (including **Recall@K** and **NDCG@K**) before selecting parameters for final training. The holdout model should **not** be used interchangeably with the production/full-data model.

## 7. Step 3 — Train ALS on the entire processed dataset

Train the final implicit-feedback ALS model with **all** rows in `gold/als_interactions` (no train/test split):

```powershell
python src/train_full_als.py --input "$base" --output "$modelRoot" --rank 16 --max-iter 10 --alpha 20 --reg 0.1 --shuffle-partitions 64
```

Explanation of the main parameters:

- `--rank 16`: size of each latent user/item vector.
- `--max-iter 10`: ALS optimization iterations.
- `--alpha 20`: confidence scaling for implicit feedback.
- `--reg 0.1`: regularization.
- `--shuffle-partitions 64`: Spark shuffle partition count.

Outputs:

```text
reports/als_real_v1/
├── model/
│   ├── metadata/
│   ├── userFactors/
│   └── itemFactors/
├── metadata.json
└── metadata_spark/
```

Check that the model was fully saved and inspect its counts:

```powershell
Test-Path "$modelRoot/model/metadata"
Test-Path "$modelRoot/model/userFactors"
Test-Path "$modelRoot/model/itemFactors"
Get-Content "$modelRoot/metadata.json"
```

Do not advance to recommendations if the model metadata or factors are missing. The counts in `metadata.json` reflect the data you actually processed, which can be a subset of the original MPD.

## 8. Step 4 — Generate playlist recommendations

Generate top-20 unseen tracks for **1,000 playlists** (useful for a local run or demo):

```powershell
python src/recommend_playlists.py --input "$base" --model "$modelRoot/model" --output "$serving" --k 20 --candidate-pool 100 --max-users 1000 --shuffle-partitions 64
```

To generate recommendations for **every playlist** with learned ALS factors, omit `--max-users`:

```powershell
python src/recommend_playlists.py --input "$base" --model "$modelRoot/model" --output "$serving" --k 20 --candidate-pool 100 --shuffle-partitions 64
```

Both commands write Parquet to:

```text
reports/serving_real_v1/playlist_recommendations/
```

The second command overwrites the first Parquet result if run with the same `$serving` location. Playlist recommendation rows contain `playlist_id` and an array of `{rank, track_id, score}`. Already-present tracks are filtered out; due to the finite candidate pool, some playlists may have fewer than 20 results. The score is an **ALS ranking score**, not a probability.

Verify the output:

```powershell
Test-Path "$serving/playlist_recommendations"
```

## 9. Step 5 — Generate similar songs using FAISS

The recommender indexes **all learned ALS item vectors** in FAISS HNSW. It computes cosine similarity using normalized latent factors. Limiting `--max-query-tracks` limits the **source songs** that receive stored recommendations; it does not limit the candidate catalog indexed by FAISS.

To reproduce our **100,000-source-song** setup, generate up to 20 neighbors for each of the 100,000 most frequent songs:

```powershell
$trackRecs = "$serving/track_similar_top100k"

python src/recommend_tracks.py --input "$base" --model "$modelRoot/model" --output "$trackRecs" --k 20 --max-query-tracks 100000 --batch-size 256 --per-file 10000
```

For an initial lightweight functional test instead, use a **different output folder**:

```powershell
python src/recommend_tracks.py --input "$base" --model "$modelRoot/model" --output "$serving/track_similar_pilot_1000" --k 20 --max-query-tracks 1000
```

To compute similar songs for **all items with learned factors** (expensive; a distributed or high-memory environment is recommended):

```powershell
python src/recommend_tracks.py --input "$base" --model "$modelRoot/model" --output "$serving/track_similar_all" --k 20 --all-tracks
```

Output example:

```text
reports/serving_real_v1/track_similar_top100k/
├── part-00000.jsonl
├── part-00001.jsonl
├── ...
└── summary.json
```

Inspect the results:

```powershell
Get-ChildItem "$trackRecs" -Filter "part-*.jsonl"
Get-Content "$trackRecs/summary.json"
```

**Important:** `recommend_tracks.py` refuses to write to an output folder already containing `part-*.jsonl` files. For a new run, use a new **empty directory**; do not mix stale or partially written shards. A successful run writes `summary.json` with `indexed_tracks` and `query_tracks_exported`.

**Coverage limitation:** If the MongoDB catalog contains approximately 1.9 million tracks but only 100,000 source tracks have recommendations, many searchable tracks will legitimately return `[]` from the recommendation endpoint. This does not, by itself, indicate a database error.

## 10. Step 6 — Start MongoDB

### Option A: MongoDB in Docker (recommended for local development)

Start Docker Desktop, then use **one** of these commands:

```powershell
# First time only — create the container:
docker run -d --name mpd-mongo -p 127.0.0.1:27017:27017 -v mpd_mongo_data:/data/db mongo:8

# On later sessions, if the container already exists and is stopped:
docker start mpd-mongo

# Confirm it is running:
docker ps
```

Do **not** run `docker run` again if the named container already exists. The named volume `mpd_mongo_data` persists your local MongoDB documents across container restarts. Binding to `127.0.0.1` is for local development; do not expose an unauthenticated MongoDB server publicly.

### Option B: MongoDB Community Server

If you already installed MongoDB as a native Windows service, start that service instead. You can use **MongoDB Compass** to inspect either a local or remote MongoDB instance.

Set the MongoDB connection and database name in the backend PowerShell session:

```powershell
$env:MONGODB_URI = "mongodb://127.0.0.1:27017"
$env:MONGODB_DB = $db
```

## 11. Step 7 — Export the catalog and recommendations to MongoDB

The exporter uses **batched upserts**, so rerunning the same import against the same dataset/model replaces documents with matching `_id` rather than creating duplicates. However, do **not** mix results from different ETL index mappings or ALS models in one database.

### Recommended: import one collection at a time

```powershell
# 1) Track catalog from Silver + Gold mapping
python src/export_mongodb.py --input "$base" --db "$db" --only tracks

# 2) Playlist catalog from Silver
python src/export_mongodb.py --input "$base" --db "$db" --only playlists

# 3) Precomputed playlist recommendations from Parquet
python src/export_mongodb.py --input "$base" --db "$db" --playlist-recs "$serving/playlist_recommendations" --only playlist_recommendations

# 4) Precomputed track recommendations from JSONL
python src/export_mongodb.py --input "$base" --db "$db" --track-recs "$trackRecs" --only track_recommendations
```

**Why separate imports?** If, for example, the fourth command fails because the JSONL folder is wrong, the three completed collections remain in MongoDB. Fix the path and **rerun only that fourth command**, not all previous steps.

### Alternative: import all four collections in one command

Run this instead of the four separate imports if all outputs are present and verified:

```powershell
python src/export_mongodb.py --input "$base" --db "$db" --playlist-recs "$serving/playlist_recommendations" --track-recs "$trackRecs"
```

The MongoDB collections are:

| Collection | Document `_id` | Data |
| --- | --- | --- |
| `tracks` | Numeric ALS `track_id` | Spotify URI, song title, artist, album, duration |
| `playlists` | Numeric `playlist_id` | Playlist metadata |
| `playlist_recommendations` | Numeric `playlist_id` | Ranked `{track_id, score}` results |
| `track_recommendations` | Numeric source `track_id` | Ranked `{track_id, similarity}` neighbors |

A unique index is created on `tracks.track_uri`; MongoDB automatically indexes each collection's `_id`. Similarity values are **not** Spotify API similarity scores; they are approximate cosine similarities between ALS item-factor vectors.

### Verify MongoDB

```powershell
python src/check_mongodb.py

docker exec -it mpd-mongo mongosh "$db"
```

Inside `mongosh` (or use MongoDB Compass's **Documents** tab to inspect the collections):

```javascript
db.tracks.countDocuments({})
db.playlists.countDocuments({})
db.playlist_recommendations.countDocuments({})
db.track_recommendations.countDocuments({})
db.track_recommendations.findOne()
```

> If you use MongoDB Compass, connect to `mongodb://127.0.0.1:27017`, open your chosen database, and inspect the four collections. `track_recommendations` holds one document per **source track**, not one document per neighbor.

## 12. Step 8 — Run the FastAPI backend

From a **new PowerShell terminal** in the `backend` directory:

```powershell
cd "C:\Users\JoanAndroidStudio\Desktop\ID2221_project\backend"
.\.venv\Scripts\Activate.ps1

$env:MONGODB_URI = "mongodb://127.0.0.1:27017"
$env:MONGODB_DB = "mpd_recommender_real_v1"

python -m uvicorn api.main:app --host 127.0.0.1 --port 8000 --reload
```

The example database name must match the one used for the import. The API performs a MongoDB `ping` at startup; if the server is unavailable or the URI is incorrect, startup fails. `--reload` is for development only.

Useful local URLs:

- Interactive Swagger documentation: <http://127.0.0.1:8000/docs>
- API health: <http://127.0.0.1:8000/api/health>
- Track search: <http://127.0.0.1:8000/api/tracks/search?q=Bohemian>

Test from another PowerShell terminal:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/health"
Invoke-RestMethod "http://127.0.0.1:8000/api/tracks/search?q=Bohemian&limit=20"
```

### API endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/health` | Check the API and MongoDB connection |
| `GET` | `/api/tracks/search?q=...&limit=20` | Search track **titles** (case-insensitive substring) |
| `GET` | `/api/recommendations/tracks?uris=spotify:track:ID1,spotify:track:ID2&k=20` | Combine and rank neighbors of selected songs |
| `GET` | `/api/playlists/search?q=...&limit=20` | Search playlist titles |
| `GET` | `/api/recommendations/playlists/{playlist_id}?k=20` | Retrieve a playlist's precomputed continuation |

For recommendations, the API resolves Spotify URIs to integer ALS track IDs, reads `track_recommendations`, excludes selected tracks, **sums** similarities across selected sources, then looks up the recommended tracks' names and Spotify URIs in `tracks`. It returns the top 20 by `totalScore` in the format expected by the frontend.

The current search searches `track_name`, **not artist names**. A track can appear in search and still have no precalculated neighbors: the API returns `[]` when none of the selected tracks has saved recommendations. Searching 1.9 million documents with an unanchored regex may be slow; production search would benefit from dedicated indexing/autocomplete.

## 13. Frontend setup (SvelteKit)

Open a **separate terminal** while keeping the FastAPI terminal running:

```powershell
cd "C:\Users\JoanAndroidStudio\Desktop\ID2221_project\frontend"

# Install dependencies (first run / after package.json changes):
npm install

# Start the SvelteKit development server:
npm run dev
```

Open the URL printed by Vite, normally <http://localhost:5173/>.

If npm reports `EBADENGINE` for `@sveltejs/kit`, check `node -v` and upgrade Node.js to a version supported by the project's `package.json` (SvelteKit 3.1.1 required Node.js **22.17.0 or newer** in the environment used here). Reopen your terminal after updating Node.

### Connect SvelteKit to FastAPI

The two original SvelteKit dummy handlers must be replaced with server-side proxy handlers:

- `src/routes/api/tracks/search/+server.ts` forwards requests to `http://127.0.0.1:8000/api/tracks/search`.
- `src/routes/api/recommendations/tracks/+server.ts` forwards requests to `http://127.0.0.1:8000/api/recommendations/tracks`.

Place handlers at your **existing** API route paths if they differ. If using the provided shared `src/lib/server/backend.ts` proxy, create `frontend/.env`:

```dotenv
BACKEND_URL=http://127.0.0.1:8000
```

Then restart the frontend dev server so its server-side environment configuration is loaded. If your handlers hardcode `http://127.0.0.1:8000`, that constant controls the URL instead of `.env`.

SvelteKit performs the HTTP requests to FastAPI **on the server**, while FastAPI queries MongoDB. MongoDB connection strings must never be exposed in client-side JavaScript.

Test the proxy using the frontend's own route, assuming that is its route path:

```text
http://localhost:5173/api/tracks/search?q=Bohemian
```

The frontend displays songs returned by MongoDB; selecting songs triggers the track recommendations endpoint.

## 14. How to run the project on subsequent days

You do **not** need to repeat ETL, training, FAISS, or MongoDB import every time. Those are **offline preparation steps**. For ordinary local development:

**Terminal 1 — MongoDB (only if stopped):**

```powershell
docker start mpd-mongo
```

**Terminal 2 — Backend API:**

```powershell
cd "C:\Users\JoanAndroidStudio\Desktop\ID2221_project\backend"
.\.venv\Scripts\Activate.ps1
$env:MONGODB_URI = "mongodb://127.0.0.1:27017"
$env:MONGODB_DB = "mpd_recommender_real_v1"
python -m uvicorn api.main:app --reload --port 8000
```

**Terminal 3 — Frontend:**

```powershell
cd "C:\Users\JoanAndroidStudio\Desktop\ID2221_project\frontend"
npm run dev
```

## 15. Troubleshooting

| Symptom | Cause and action |
| --- | --- |
| `FileNotFoundException: data/raw/mpd.slice.*.json` | No matching files, incorrect extraction directory, or Hadoop glob resolution on Windows. Verify filenames and try `--input "data/raw"`. |
| `NativeIO$Windows.access0` / `winutils.exe` error | Inspect `HADOOP_HOME`, `PATH`, `winutils.exe`, and matching `hadoop.dll`; ensure binary versions are compatible. |
| `Python worker failed to connect back` | Set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to the full `.venv\Scripts\python.exe` path. |
| ALS model `metadata` path not found | The model was not saved to the `--model` path. Check `$modelRoot/model/metadata` and training logs. |
| `Output has ... existing JSONL shards` | `recommend_tracks.py` requires a new empty output directory. Do not reuse old or partially written shard directories. |
| `No part-*.jsonl in ...` during Mongo export | The specified `--track-recs` folder is wrong or track generation did not finish. Inspect `summary.json` and the directory. |
| Search returns tracks but recommendations are `[]` | Search covers the catalog; only tracks with documents in `track_recommendations` can act as recommendation sources. Also verify the same ID mapping and Mongo database. |
| Search for an artist name yields no results | The current backend searches track titles, not `artist_name`. |
| API cannot connect to MongoDB | Check `docker ps`, `MONGODB_URI`, port 27017, and the exact database in `MONGODB_DB`. |
| SvelteKit `EBADENGINE` | Upgrade Node.js to the version required by `@sveltejs/kit` and rerun `npm install`. |
| API returns 502 through SvelteKit | Ensure FastAPI is running on port 8000 and the SvelteKit proxy targets the correct URL. |
| Spark / FAISS runs out of memory | Use smaller pilot counts or provision a higher-memory/distributed environment. The full MPD is substantially larger than synthetic tests. |

### Reimport only track recommendations after a partial failure

If the other collections were successfully imported, **do not reimport them unnecessarily**:

```powershell
$base = "data/processed_real_v1"
$db = "mpd_recommender_real_v1"
$trackRecs = "reports/serving_real_v1/track_similar_top100k"
$env:MONGODB_URI = "mongodb://127.0.0.1:27017"

python src/export_mongodb.py --input "$base" --db "$db" --track-recs "$trackRecs" --only track_recommendations
```

## 16. Reproducibility and limitations

- The training signal is **implicit playlist membership** (`rating = 1.0`), not explicit user ratings or Spotify listening history.
- ALS playlist recommendations and FAISS track similarity are two distinct recommendation methods using the same trained item factors.
- FAISS HNSW provides **approximate** nearest neighbors. `similarity` represents cosine similarity between latent item factors; ALS `score` is a ranking score, not a probability.
- The 100,000-track setting means **100,000 source songs with up to 20 recommended neighbors each**, not 100,000 recommendations in total. The exact number depends on successful processing and import.
- Generating only 1,000 playlist recommendations and 100,000 source-track recommendations does **not** mean every catalog item has precomputed results.
- ETL recreates numeric track IDs through `StringIndexer`. Keep the associated ETL tables, ALS model, JSONL/Parquet results, and MongoDB database from the **same run**.
- Keep the original MPD files, generated data, database credentials, and large model artifacts out of Git unless there is a deliberate data-sharing and licensing plan.
- Local Docker MongoDB without authentication and `uvicorn --reload` are suitable **only for local development**, not production deployment.

## 17. Technology references

- [Apache Spark PySpark documentation](https://spark.apache.org/docs/latest/api/python/)
- [Spark ALS model](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.recommendation.ALS.html)
- [FAISS documentation](https://github.com/facebookresearch/faiss/wiki)
- [MongoDB documentation](https://www.mongodb.com/docs/)
- [FastAPI documentation](https://fastapi.tiangolo.com/)
- [SvelteKit documentation](https://svelte.dev/docs/kit/introduction)

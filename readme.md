# 🎵 Spotify End-to-End Data Pipeline on AWS

> A production-grade, fully serverless ETL pipeline that ingests Spotify data via the Spotify Web API, stores and transforms it on AWS, and makes it queryable via SQL analytics — all triggered automatically on a schedule.

---

## 📌 Table of Contents

- [Project Overview](#-project-overview)
- [Architecture Diagram](#-architecture-diagram)
- [Tech Stack](#-tech-stack)
- [Pipeline Walkthrough](#-pipeline-walkthrough)
  - [Phase 1 — Extract](#phase-1--extract)
  - [Phase 2 — Transform](#phase-2--transform)
  - [Phase 3 — Load & Catalog](#phase-3--load--catalog)
  - [Phase 4 — Analytics](#phase-4--analytics)
- [S3 Folder Structure](#-s3-folder-structure)
- [Data Models](#-data-models)
- [How to Run](#-how-to-run)
- [Key Design Decisions](#-key-design-decisions)
- [Lessons Learned](#-lessons-learned)

---

## 🧭 Project Overview

This project builds a fully automated, cloud-native data pipeline that extracts data from the **Spotify Web API** (songs, artists, and albums), stores raw JSON in **Amazon S3**, transforms it into structured CSVs using **AWS Lambda**, catalogs the schema via **AWS Glue Crawler**, and enables SQL analytics through **Amazon Athena**.

The pipeline runs on a **daily schedule** via **Amazon CloudWatch Events**, making it suitable for tracking music trends over time with zero manual intervention.

| Dimension | Detail |
|---|---|
| **Data Source** | Spotify Web API (personal library — albums, tracks, artists) |
| **Ingestion Pattern** | Scheduled pull (daily via CloudWatch) |
| **Storage Layer** | Amazon S3 (raw JSON → processed CSV) |
| **Compute** | AWS Lambda (Python 3.x, serverless) |
| **Schema Management** | AWS Glue Crawler + Data Catalog |
| **Analytics** | Amazon Athena (SQL over S3) |
| **Orchestration** | CloudWatch Events + S3 Object Put Trigger |

---

## 🏗 Architecture Diagram

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                              AWS Cloud                                         │
│                                                                                │
│   ┌─────────────┐     ┌──────────────────┐     ┌───────────────────────────┐  │
│   │  CloudWatch │────▶│  AWS Lambda      │────▶│  Amazon S3                │  │
│   │  (daily     │     │  (data           │     │  s3://bucket/             │  │
│   │  schedule)  │     │   extraction)    │     │  ├── to_process/          │  │
│   └─────────────┘     └──────────────────┘     │  └── processed/          │  │
│                               │                └───────────┬───────────────┘  │
│                               │ Spotify API                │ S3 Object Put     │
│                               ▼                            │ Trigger           │
│                        ┌─────────────┐                     ▼                  │
│                        │  Raw JSON   │          ┌──────────────────────┐       │
│                        │  stored in  │          │  AWS Lambda          │       │
│                        │  S3         │          │  (data               │       │
│                        └─────────────┘          │   transformation)    │       │
│                                                 └──────────┬───────────┘       │
│                                                            │                   │
│                                                            ▼                   │
│   ┌────────────────────────────────────────────────────────────────────────┐   │
│   │                    Amazon S3 (transformed data)                        │   │
│   │   s3://bucket/transformed/                                             │   │
│   │   ├── albums/albums_data.csv                                           │   │
│   │   ├── artists/artists_data.csv                                         │   │
│   │   └── songs/songs_data.csv                                             │   │
│   └───────────────────────────┬────────────────────────────────────────────┘   │
│                               │                                                │
│               ┌───────────────┴─────────────────────┐                         │
│               ▼                                     ▼                         │
│   ┌───────────────────────┐           ┌─────────────────────────┐             │
│   │  AWS Glue Crawler     │──────────▶│  AWS Glue Data Catalog  │             │
│   │  (infers schema from  │           │  (auto-creates tables)  │             │
│   │   CSV files)          │           └──────────────┬──────────┘             │
│   └───────────────────────┘                          │                        │
│                                                      ▼                        │
│                                          ┌───────────────────────┐            │
│                                          │  Amazon Athena        │            │
│                                          │  (SQL analytics)      │            │
│                                          └───────────────────────┘            │
└────────────────────────────────────────────────────────────────────────────────┘

External:  Spotify API ──────────────────────────────────────────────────────▶
```

---

## 🛠 Tech Stack

| Layer | Service / Tool |
|---|---|
| **API Client** | Python + `spotipy` library (Spotify OAuth) |
| **Scheduler** | Amazon CloudWatch Events (cron rule) |
| **Extraction Lambda** | Python 3.x, `spotipy`, `boto3`, `json` |
| **Raw Storage** | Amazon S3 (`to_process/` folder) |
| **Trigger** | S3 Event Notification (ObjectCreated) |
| **Transform Lambda** | Python 3.x, `pandas`, `boto3` |
| **Processed Storage** | Amazon S3 (`processed/`, `transformed/` folders) |
| **Schema Catalog** | AWS Glue Crawler + Glue Data Catalog |
| **Analytics** | Amazon Athena (Presto SQL engine) |
| **Prototyping** | Google Colab (used for initial API testing & transformation logic) |

---

## 🔄 Pipeline Walkthrough

### Phase 1 — Extract

**Trigger:** Amazon CloudWatch fires a daily EventBridge rule (cron expression).

**Lambda — `spotify_extract_lambda`:**
1. Authenticates with Spotify Web API using OAuth 2.0 (Client Credentials / Authorization Code flow).
2. Calls the Spotify API endpoints to fetch:
   - Saved albums
   - Album tracks (song metadata)
   - Artist details
3. Captures the raw API response as a Python dictionary.
4. Serializes it to JSON and writes it to:
   ```
   s3://your-bucket/to_process/spotify_raw_YYYY-MM-DD.json
   ```

**Key insight:** Spotify's API only exposes *personal* library data (saved albums, liked songs) — not public playlist data — when using Authorization Code flow with user scopes. This was discovered during prototyping in Google Colab.

---

### Phase 2 — Transform

**Trigger:** S3 Object Put event fires when a new JSON file lands in `to_process/`.

**Lambda — `spotify_transform_lambda`:**
1. Reads the raw JSON from S3.
2. Flattens and normalizes the nested API response into three separate entities:

| Entity | Fields |
|---|---|
| **Albums** | `album_id`, `album_name`, `release_date`, `total_tracks`, `url` |
| **Artists** | `artist_id`, `artist_name`, `external_url` |
| **Songs** | `song_id`, `song_name`, `duration_ms`, `explicit`, `added_at`, `album_id`, `artist_id` |

3. Writes each entity as a CSV file to:
   ```
   s3://your-bucket/transformed/albums/
   s3://your-bucket/transformed/artists/
   s3://your-bucket/transformed/songs/
   ```
4. Moves the processed JSON from `to_process/` → `processed/` to prevent reprocessing.

---

### Phase 3 — Load & Catalog

**AWS Glue Crawler** is configured to:
- Point to all three `transformed/` sub-folders in S3.
- Auto-detect CSV schema (column names, data types).
- Run on a schedule (or manually triggered after transformation).
- Populate the **AWS Glue Data Catalog** with three tables: `albums`, `artists`, `songs`.

This step eliminates the need to manually define schemas — the crawler infers them directly from the data files.

---

### Phase 4 — Analytics

**Amazon Athena** reads directly from S3 using the Glue Data Catalog as a metastore.

Sample queries you can run:

```sql
-- Top 10 most recently added songs
SELECT song_name, added_at, artist_id
FROM songs
ORDER BY added_at DESC
LIMIT 10;

-- Albums with the most tracks
SELECT a.album_name, a.total_tracks, a.release_date
FROM albums a
ORDER BY a.total_tracks DESC
LIMIT 10;

-- Join songs with artists
SELECT s.song_name, ar.artist_name, s.duration_ms / 1000 AS duration_seconds
FROM songs s
JOIN artists ar ON s.artist_id = ar.artist_id
ORDER BY s.duration_ms DESC
LIMIT 20;
```

---

## 🗂 S3 Folder Structure

```
s3://your-spotify-bucket/
│
├── to_process/                          ← Raw JSON lands here (Lambda writes)
│   └── spotify_raw_2026-05-07.json
│
├── processed/                           ← JSON moved here after transformation
│   └── spotify_raw_2026-05-06.json
│
└── transformed/                         ← Cleaned CSVs for Athena queries
    ├── albums/
    │   └── albums_data.csv
    ├── artists/
    │   └── artists_data.csv
    └── songs/
        └── songs_data.csv
```

---

## 📊 Data Models

### `albums`
| Column | Type | Description |
|---|---|---|
| `album_id` | STRING | Spotify unique album ID |
| `album_name` | STRING | Album title |
| `release_date` | STRING | Release date (YYYY-MM-DD) |
| `total_tracks` | INT | Number of tracks |
| `url` | STRING | Spotify external URL |

### `artists`
| Column | Type | Description |
|---|---|---|
| `artist_id` | STRING | Spotify unique artist ID |
| `artist_name` | STRING | Artist name |
| `external_url` | STRING | Spotify artist URL |

### `songs`
| Column | Type | Description |
|---|---|---|
| `song_id` | STRING | Spotify unique track ID |
| `song_name` | STRING | Track title |
| `duration_ms` | INT | Duration in milliseconds |
| `explicit` | BOOLEAN | Explicit content flag |
| `added_at` | TIMESTAMP | When track was added to library |
| `album_id` | STRING | Foreign key → albums |
| `artist_id` | STRING | Foreign key → artists |

---

## 🚀 How to Run

### Prerequisites
- AWS account with permissions for: Lambda, S3, CloudWatch, Glue, Athena
- Spotify Developer account → [Create an App](https://developer.spotify.com/dashboard)
- Python 3.9+

### Step 1 — Spotify API Setup
```bash
# Install dependencies for local testing
pip install spotipy boto3 pandas
```

Create a `.env` file (never commit this):
```env
SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
SPOTIFY_REDIRECT_URI=https://example.com/callback
```

### Step 2 — Deploy Extraction Lambda
1. Package `lambda_extract.py` + `spotipy` layer into a ZIP.
2. Upload to AWS Lambda (Python 3.9 runtime).
3. Set environment variables: `CLIENT_ID`, `CLIENT_SECRET`, `BUCKET_NAME`.
4. Add IAM role with `s3:PutObject` permission.

### Step 3 — Configure CloudWatch Trigger
```
Cron expression: cron(0 8 * * ? *)   ← Runs daily at 8AM UTC
```

### Step 4 — Deploy Transformation Lambda
1. Package `lambda_transform.py` + `pandas` layer.
2. Upload to AWS Lambda.
3. Add S3 trigger: Event type = `s3:ObjectCreated:*`, Prefix = `to_process/`.
4. Add IAM role with `s3:GetObject` + `s3:PutObject` + `s3:DeleteObject`.

### Step 5 — Configure Glue Crawler
1. Create a Glue Crawler pointing to `s3://your-bucket/transformed/`.
2. Set schedule (e.g., daily after Lambda runs).
3. Output database: `spotify_db`.
4. Run crawler — it will create `albums`, `artists`, `songs` tables.

### Step 6 — Query in Athena
1. Open Amazon Athena → select `spotify_db` database.
2. Set S3 output location for query results.
3. Run SQL queries on `albums`, `artists`, `songs` tables.

---

## 💡 Key Design Decisions

| Decision | Rationale |
|---|---|
| **Serverless Lambda over EC2** | No idle compute cost; scales to zero when not running. |
| **S3 as the data lake** | Decouples storage from compute; Athena queries directly on S3. |
| **S3 Event trigger (not polling)** | Event-driven design; transformation fires immediately when data arrives — no scheduled delay. |
| **`to_process/` → `processed/` pattern** | Idempotency guard — prevents the same file from being transformed twice. |
| **Glue Crawler for schema inference** | Eliminates hardcoded DDL; handles schema evolution automatically as API response changes. |
| **Separate CSVs per entity** | Normalized model enables clean JOINs in Athena and avoids data duplication. |

---

## 📖 Lessons Learned

1. **Spotify API scopes matter** — The API only returns *personal* library data (saved albums, liked songs) under the Authorization Code flow. Public playlist data requires different endpoints and scopes. This was discovered during local prototyping in Google Colab before deploying to Lambda.

2. **Prototyping in Colab first saved time** — Testing the raw API response structure and transformation logic in Colab before writing Lambda code avoided costly debug cycles in the cloud.

3. **S3 event triggers are powerful** — Using an S3 ObjectCreated event to chain the two Lambdas creates a clean, event-driven architecture without needing Step Functions or a workflow orchestrator for this scale.

4. **IAM least privilege** — Each Lambda function was given only the S3 permissions it needed (extract Lambda: PutObject only; transform Lambda: GetObject + PutObject + DeleteObject). This is a production best practice that prevents accidental data deletion or exposure.

5. **Glue Crawler schema drift** — If the Spotify API response structure changes (e.g., new fields added), the crawler auto-detects and updates the schema — no manual ALTER TABLE needed in Athena.

---

## 📁 Repository Structure

```
spotify-aws-pipeline/
│
├── lambda/
│   ├── extract/
│   │   └── lambda_function.py        ← Spotify API extraction logic
│   └── transform/
│       └── lambda_function.py        ← JSON → CSV transformation logic
│
├── notebooks/
│   └── spotify_exploration.ipynb     ← Google Colab prototyping notebook
│
├── athena_queries/
│   ├── top_songs.sql
│   ├── album_stats.sql
│   └── artist_join.sql
│
├── architecture/
│   └── aws_etl_diagram.png           ← Architecture diagram
│
├── requirements.txt                  ← Python dependencies
└── README.md
```

---

## 🔐 Security Notes

- **Never commit** Spotify `CLIENT_ID` / `CLIENT_SECRET` to Git. Use AWS Secrets Manager or Lambda environment variables.
- Enable **S3 bucket versioning** to protect against accidental overwrites.
- Use **S3 bucket policies** to block public access.
- Consider enabling **CloudTrail** for audit logging of all S3 and Lambda activity.

---

*Built with Python, AWS Lambda, S3, Glue, Athena, and the Spotify Web API.*

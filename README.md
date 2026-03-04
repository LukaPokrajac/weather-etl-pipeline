# Wear index ETL pipeline

Dockerized data pipeline that ingest hourly weather data from Open-Meteo stores it in Postgresql and computes a wear index using window functions.

# Problem

Calculate feels like temperature based on smoothed temp and wind to know what to wear.

# Architecture

Open-Meteo API
Python ETL
Staging table
UPSERT into weather_hourly
Materialized view wear_now

# Tech Stack

Python (requests, pandas, SQLAlchemy, logging)
PostgreSQL 
Docker
Materialized Views
Window Functions
Cron scheduling

# Run locally

1. Start Postgres
   docker-compose up -d
 
2. Run ETL
   python load_pipeline.py

3. Query wear index
   curl http://localhost:8000/wear

# Example output

select anchor_ts, feels_like_c, label
from wear_now
order by anchor_ts
limit 1;

       anchor_ts        | feels_like_c | label
------------------------+--------------+-------
 2026-02-12 01:00:00+00 |          5.1 | Cold
(1 row)

# Design Decisions

- Used UPSERT to make loads idempotent.
- Added 3-hour overlap in incremental loading to avoid boundary gaps.
- Used window functions to smooth temperature noise.
- Created materialized view for fast serving instead of computing on request.
- Indexed anchor_ts for low-latency lookup.

# Future Improvements

- Add Airflow for orchestration
- Add monitoring / logging
- Deploy API to cloud (Render / Fly.io)
- Add unit tests
- Add FastAPI
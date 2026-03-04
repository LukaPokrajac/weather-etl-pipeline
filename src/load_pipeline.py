
from datetime import datetime, timezone
import pandas as pd
import requests
from sqlalchemy import create_engine, text
import logging
import math

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

DB_URL = 'postgresql+psycopg://etl:etl@localhost:5432/weather'
OPEN_METEO_URL = 'https://api.open-meteo.com/v1/forecast'
LAT, LON = 44.8176, 20.4633 # Belgrade-ish

def fetch_open_meteo(past_days: int, forecast_days: int) -> dict:
    params = {
        'latitude': LAT,
        'longitude': LON,
        'hourly':'temperature_2m,precipitation,windspeed_10m',
        'timezone':'UTC',
        'past_days': past_days,
        'forecast_days': forecast_days,
    }

    r = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def build_hourly_df(payload: dict, fetched_at: datetime) -> pd.DataFrame:
    hourly = payload['hourly']
    
    df = pd.DataFrame({
        'ts': pd.to_datetime(hourly['time'], utc=True),
        'temperature_c': hourly['temperature_2m'],
        'precipitation_mm': hourly.get('precipitation'), #.get() because precipitaton is optional and etl doesnt have to fail loudly if it fails
        'windspeed_kmh': hourly['windspeed_10m'],
    })

    df['source_fetched_at'] = pd.to_datetime(fetched_at, utc=True)

    df=df.dropna(subset=['ts']).drop_duplicates(subset=['ts'])
    return df

def load_df_to_postgres(df: pd.DataFrame) -> None:
    engine = create_engine(DB_URL)
    #keeping only columns that exist in staging schema
    df = df[['ts', 'temperature_c', 'precipitation_mm', 'windspeed_kmh', 'source_fetched_at']].copy()

    with engine.begin() as conn:
        #staging
        conn.execute(text('truncate table stg_weather_hourly;'))

        #load into staging
        df.to_sql('stg_weather_hourly', conn, if_exists='append', index=False)

        #upsert into final table
        conn.execute(text('''
            insert into weather_hourly (ts, temperature_c, precipitation_mm, windspeed_kmh, source_fetched_at)
            select ts, temperature_c, precipitation_mm, windspeed_kmh, source_fetched_at
            from stg_weather_hourly 
            on conflict (ts) do update set
                temperature_c = excluded.temperature_c,
                precipitation_mm =excluded.precipitation_mm,
                windspeed_kmh = excluded.windspeed_kmh,
                source_fetched_at = excluded.source_fetched_at;                                
        '''))
        conn.execute(text("refresh materialized view wear_now;"))

def get_max_ts(engine) -> pd.Timestamp | None:
    with engine.connect() as conn:
        val = conn.execute(text("select max(ts) from weather_hourly")).scalar()
    if val is None:
        return None
    return pd.Timestamp(val).tz_convert("UTC") if pd.Timestamp(val).tzinfo else pd.Timestamp(val, tz="UTC")

def main():
    engine = create_engine(DB_URL)

    now = pd.Timestamp(datetime.now(timezone.utc)).floor("h")
    max_ts = get_max_ts(engine)

    # Default small window for fresh DB
    if max_ts is None:
        past_days = 2
    else:
        # include overlap so we safely cover boundary hours
        overlap_hours = 3
        effective_start = max_ts - pd.Timedelta(hours=overlap_hours)

        hours_behind = max(pd.Timedelta(0), now - effective_start)
        days_behind = math.ceil(hours_behind / pd.Timedelta(days=1))

        # past_days must be >= 1; cap so you don't pull huge history
        past_days = max(1, min(7, days_behind))

    forecast_days = 2  # enough for t+1 wear index, tune if you want more horizon

    fetched_at = datetime.now(timezone.utc)
    payload = fetch_open_meteo(past_days=past_days, forecast_days=forecast_days)
    df = build_hourly_df(payload, fetched_at)
    load_df_to_postgres(df)
    logging.info(f"OK: staged + upserted {len(df)} rows")
    print(f"OK: staged + upserted {len(df)} rows")


if __name__ == "__main__":
    main()
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg://etl:etl@localhost:5432/weather"

# weights for 7 hours: t..t+7 centered on t
WEIGHTS = [5, 5, 4, 2, 2, 1, 1]
WIND_K = 0.1  # °C penalty per 1 km/h wind (tune later)

@dataclass
class WearResult:
    anchor_ts: pd.Timestamp
    feels_like_c: float
    label: str


def label_from_feels_like(feels_like_c: float) -> str:
    # Tune these thresholds later based on your personal feel.
    thresholds = [(0, 'Freezing'),
                  (8, 'Cold'), 
                  (15, 'Chilly'), 
                  (22,"Mild"),
                  (float("Inf"), 'Hot')
    ]
    for temp, label in thresholds:
        if temp > feels_like_c:
            return label


def fetch_window(engine, anchor_ts: pd.Timestamp) -> pd.DataFrame:
    """Fetch exactly 7 hourly rows: anchor .. anchor+6h."""
    start = anchor_ts.to_pydatetime()
    end = (anchor_ts + pd.Timedelta(hours=6)).to_pydatetime()

    q = text("""
        select ts, temperature_c, windspeed_kmh
        from weather_hourly
        where ts >= :start_ts and ts <= :end_ts
        order by ts asc
    """)

    df = pd.read_sql(q, engine, params={"start_ts": start, "end_ts": end})
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def compute_wear_index(df: pd.DataFrame, anchor_ts: pd.Timestamp) -> WearResult:
    
    if len(df) != 7:
        raise ValueError(
            f"Expected 7 hourly rows (t..t+6), got {len(df)} around anchor {anchor_ts}."
        )
    
    df = df.copy()
    df["w"] = WEIGHTS

    temp_smooth = float((df["temperature_c"] * df["w"]).sum() / df["w"].sum())
    wind_smooth = float((df["windspeed_kmh"] * df["w"]).sum() / df["w"].sum())

    feels_like = temp_smooth - WIND_K * wind_smooth
    label = label_from_feels_like(feels_like)

    return WearResult(
        anchor_ts=anchor_ts,
        feels_like_c=float(feels_like),
        label=label,
    )


def main() -> None:
    engine = create_engine(DB_URL)

    now = pd.Timestamp(datetime.now(timezone.utc))
    anchor = now.floor("h")

    df = fetch_window(engine, anchor)
    res = compute_wear_index(df, anchor)

    # Output: one number + one label
    print(f"{res.feels_like_c:.1f}°C   {res.label}")


if __name__ == "__main__":
    main()

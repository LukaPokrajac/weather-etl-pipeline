create table if not exists weather_hourly (
  ts timestamptz primary key,
  temperature_c double precision,
  precipitation_mm double precision,
  windspeed_kmh double precision,
  source_fetched_at timestamptz not null
);
create table if not exists stg_weather_hourly (
  ts timestamptz,
  temperature_c double precision,
  precipitation_mm double precision,
  windspeed_kmh double precision,
  source_fetched_at timestamptz
);

create table if not exists weather_daily as
select
  date_trunc('day', ts) as day,
  avg(temperature_c) as avg_temp_c,
  sum(precipitation_mm) as total_precip_mm,
  avg(windspeed_kmh) as avg_windspeed_kmh
from weather_hourly
group by 1;

-- refresh (simple way)
truncate table weather_daily;
insert into weather_daily
select
  date_trunc('day', ts) as day,
  avg(temperature_c),
  sum(precipitation_mm),
  avg(windspeed_kmh)
from weather_hourly
group by 1;

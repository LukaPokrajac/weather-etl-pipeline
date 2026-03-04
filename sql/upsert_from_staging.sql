insert into weather_hourly (ts, temperature_c)
select ts, temperature_c
from stg_weather_hourly
on conflict (ts) do update
set temperature_c = excluded.temperature_c;
truncate table stg_weather_hourly;

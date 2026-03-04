drop materialized view if exists wear_now;

create materialized view wear_now as
with smoothed as (
  select
    ts,
    avg(temperature_c) over (order by ts rows between 3 preceding and 3 following) as temp7,
    avg(windspeed_kmh)  over (order by ts rows between 3 preceding and 3 following) as wind7
  from weather_hourly
),
wear as (
  select
    lead(ts, 1) over (order by ts) as anchor_ts,
    lead(temp7, 1) over (order by ts) - 0.07 * lead(wind7, 1) over (order by ts) as feels_like_c
  from smoothed
)
select
  anchor_ts,
  round(feels_like_c::numeric, 1) as feels_like_c,
  case
    when feels_like_c < 0 then 'Freezing'
    when feels_like_c < 8 then 'Cold'
    when feels_like_c < 14 then 'Chilly'
    when feels_like_c < 22 then 'Mild'
    else 'Hot'
  end as label
from wear;

create index if not exists wear_now_anchor_idx on wear_now (anchor_ts);
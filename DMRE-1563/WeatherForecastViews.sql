create view "vw_etl_weather_postcode_sectors" as
  SELECT left("postcode 1", len("postcode 1") - 3) postcode
  FROM aws_s3_stage2_extracts.stage2_postcodes
  group by left("postcode 1", len("postcode 1") - 3)
  UNION
  SELECT "postcode 1" postcode
  FROM aws_s3_stage2_extracts.stage2_postcodes
  Where "postcode 1" in
      ('IP7 7RE', 'NG16 1JF', 'TW6 2EQ', 'LL18 5YD', 'B46 3JH', 'NE15 0RH', 'RH6 0EN', 'CF62 4JD', 'BA22 8HT',
       'HU17 7LZ', 'PA7 5NX', 'AB21 7DU', 'CM6 3TH', 'PE8 6HB', 'CH4 0GZ', 'CV23 9EU', 'NE66 3JF', 'WA14 3SB',
       'HA4 6NG', 'CR8 5EG', 'SP4 0JF', 'DL7 9NJ', 'KA9 2PL', 'IV36 3UH')
  Order by postcode
;

alter table vw_etl_weather_postcode_sectors owner to igloo
;

create view vw_etl_weather_forecast_hourly_load as
  select outcode,
         city_name,
         lon,
         timezone,
         lat,
         country_code,
         state_code,
         forecast_issued,
         wind_cdir,
         rh,
         pod,
         timestamp_utc,
         pres,
         solar_rad,
         ozone,
         icon,
         code,
         description,
         wind_gust_spd,
         timestamp_local,
         snow_depth,
         clouds,
         ts,
         wind_spd,
         pop,
         wind_cdir_full,
         slp,
         dni,
         dewpt,
         snow,
         uv,
         wind_dir,
         clouds_hi,
         precip,
         vis,
         dhi,
         app_temp,
         "datetime",
         temp,
         ghi,
         clouds_mid,
         clouds_low,
         getdate() as etlchange
  from aws_s3_stage2_extracts.stage2_weatherforecast48hr
  where forecast_issued :: timestamp >= dateadd(months, -1, getdate())
  WITH NO SCHEMA BINDING
;

alter table vw_etl_weather_forecast_hourly_load owner to igloo_john_steel
;

create or replace view vw_etl_weather_forecast_daily_load as
  select outcode,
         city_name,
         lon,
         "timezone",
         lat,
         country_code,
         state_code,
         forecast_issued,
         moonrise_ts,
         wind_cdir,
         rh,
         pres,
         high_temp,
         sunset_ts,
         ozone,
         moon_phase,
         wind_gust_spd,
         snow_depth,
         clouds,
         ts,
         sunrise_ts,
         app_min_temp,
         wind_spd,
         pop,
         wind_cdir_full,
         slp,
         moon_phase_lunation,
         valid_date,
         app_max_temp,
         vis,
         dewpt,
         snow,
         uv,
         wind_dir,
         max_dhi,
         clouds_hi,
         precip,
         low_temp,
         max_temp,
         moonset_ts,
         "datetime",
         "temp",
         min_temp,
         clouds_mid,
         clouds_low,
         getdate() etlchange
  from aws_s3_stage2_extracts.stage2_weatherforecast16day
  where forecast_issued :: timestamp >= dateadd(months, -1, getdate())
  with no schema binding
;

alter table vw_etl_weather_forecast_daily_load owner to igloo
;


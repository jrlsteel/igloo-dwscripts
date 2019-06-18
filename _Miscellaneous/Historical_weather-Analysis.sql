select t1.*,
       (select SUM(15.5 - rhw.temp) hdd from ref_historical_weather rhw
       where rhw.timestamp_local between t1.rd_p and  t1.meterreadingdatetime and postcode='SL6') tw
from (
       select account_id,
              meterpointnumber,
              meterreadingcreateddate,
              meterreadingdatetime,
              meterreadingsourceuid,
              readingvalue,
              (select top 1 readingvalue
               from ref_readings_internal rri2
               where rri2.account_id = rri1.account_id
                 and rri2.meterpointnumber = rri1.meterpointnumber
                 and rri2.meterreadingdatetime < rri1.meterreadingdatetime
               order by rri2.meterreadingdatetime desc) as rv_p,
              (select top 1 rri3.meterreadingdatetime
               from ref_readings_internal rri3
               where rri3.account_id = rri1.account_id
                 and rri3.meterpointnumber = rri1.meterpointnumber
                 and rri3.meterreadingdatetime < rri1.meterreadingdatetime
               order by rri3.meterreadingdatetime desc) as rd_p
       from ref_readings_internal rri1
       where meterreadingstatusuid = 'VALID'
         and account_id = 1834
         and meterreadingtypeuid = 'ACTUAL'
         and meterpointtype = 'G'
       --order by meterreadingdatetime asc
     ) t1;

select etlchange, etlchangetype, count(*) from ref_historical_weather_audit group by etlchange, etlchangetype
order by etlchange desc;

select trunc(timestamp_local), postcode
from ref_historical_weather
where postcode = 'SL6'
--   and trunc(timestamp_utc) = '2018-05-01'
group by trunc(timestamp_local), postcode
order by trunc(timestamp_local) desc, postcode asc
-- limit 1000
;

select left(timestamp_utc, 10), postcode
from aws_s3_stage2_extracts.stage2_historicalweather
where postcode = 'SL6' and trim(left(timestamp_utc,4)) = '2019'
group by left(timestamp_utc, 10), postcode
order by left(timestamp_utc, 10) desc, postcode asc
limit 1000;

select * from ref_cdb_addresses where postcode like '%SL6%';


select postcode, trunc(timestamp_utc) time_hw, trunc(etlchange) time_etl
from ref_historical_weather_audit
where postcode = 'SL6'
group by  postcode, trunc(timestamp_utc), trunc(etlchange)
order by trunc(etlchange) desc, trunc(timestamp_utc)
;

select etlchange, etlchangetype, count(*) from ref_cdb_addresses_audit
group by etlchange, etlchangetype
order by etlchange desc, etlchangetype desc;


select
count(*)
from aws_s3_stage2_extracts.stage2_historicalweather s;
--35248416

select
count(*)
from ref_historical_weather;
-- 19568016

select
addr.postcode, left(s.timestamp_local, 10)
from aws_s3_stage2_extracts.stage2_historicalweather s
inner join (SELECT substring(postcode, 1, length(postcode) - 3) as postcode
        FROM ref_cdb_addresses
        group by substring(postcode, 1, length(postcode) - 3)
        order by substring(postcode, 1, length(postcode) - 3)) addr on trim (addr.postcode) = trim (s.postcode)
where trim(addr.postcode) = 'SL6'
group by addr.postcode,left(s.timestamp_local, 10)
order by addr.postcode,left(s.timestamp_local, 10) desc ;

select * from aws_s3_stage1_extracts.stage1_historicalweather s
where postcode = 'SL6'
  limit 10;
--   and left(timestamp_utc, 4) = '2019';

select
s.postcode, left(s.timestamp_local, 10)
from aws_s3_stage2_extracts.stage2_historicalweather s
-- inner join (SELECT substring(postcode, 1, length(postcode) - 3) as postcode
--         FROM ref_cdb_addresses
--         group by substring(postcode, 1, length(postcode) - 3)
--         order by substring(postcode, 1, length(postcode) - 3)) addr on trim (addr.postcode) = trim (s.postcode)
where trim(addr.postcode) = 'SL6'
group by addr.postcode,left(s.timestamp_local, 10)
order by addr.postcode,left(s.timestamp_local, 10) desc;
;

select count(*) from ref_meterpoints where meterpointtype = 'E';
select count(*) from ref_registrations_meterpoints_status_elec; -- 35040

select account_id,meterpointnumber, count(*) from ref_registrations_meterpoints_status_elec
group by account_id,meterpointnumber
having count(*)>1; -- 0


-- Total records
; --
select trunc(timestamp_utc), count(*) from ref_historical_weather
group by trunc(timestamp_utc)
having count(*) < 56832
order by trunc(timestamp_utc) asc

select left(timestamp_utc, 10), count(*) from aws_s3_stage2_extracts.stage2_historicalweather
group by left(timestamp_utc, 10)
having count(*) > 56832
order by left(timestamp_utc, 10) asc;



select count(distinct substring(postcode, 1, length(postcode) - 3)) from ref_cdb_addresses

-- Duplicate records
-- select min(timestamp_utc), max(timestamp_utc) from (
select postcode, timestamp_utc,  count(*) from ref_historical_weather
-- where postcode = 'BN14'
group by postcode, timestamp_utc
having count(*) > 1
order by postcode, timestamp_utc desc

; --26434

select postcode, timestamp_utc,  count(*) from aws_s3_stage2_extracts.stage2_historicalweather
-- where
--     postcode = 'BN14'
group by postcode, timestamp_utc
having count(*) > 1
order by postcode, timestamp_utc desc;

select * from ref_historical_weather
where postcode = 'BN14' and trunc(timestamp_utc) = '2019-03-17'
order by postcode, timestamp_utc desc;

select * from aws_s3_stage2_extracts.stage2_historicalweather
where postcode = 'BN14' and left(timestamp_utc, 10) = '2019-03-17'
order by postcode, timestamp_utc desc;

select count(*) from aws_s3_stage2_extracts.stage2_historicalweather;
select count(*) from ref_historical_weather;

select left(timestamp_utc,10) utc, postcode, *
from aws_s3_stage2_extracts.stage2_historicalweather;

-- To check records shifted because of additional column--
select dewpt from aws_s3_stage2_extracts.stage2_historicalweather
where postcode like '%-%'
group by dewpt
order by dewpt
; -- 0


select
trim( s.postcode),
trim( s.datetime),
cast( s.timestamp_local as timestamp),
cast( s.timestamp_utc as timestamp),
trim( s.ts),
trim( s.timezone),
trim( s.country_code),
trim( s.state_code),
trim( s.city_name),
trim( s.city_id),
cast( s.lat as double precision),
cast( s.lon as double precision),
trim( s.station_id),
trim( s.sources),
cast( s.rh as double precision),
cast( s.wind_spd as double precision),
cast( s.slp as double precision),
cast( s.h_angle as double precision),
cast( s.elev_angle as double precision),
cast( s.azimuth as double precision),
cast( s.dewpt as double precision),
cast( s.snow as double precision),
cast( s.uv as double precision),
cast( s.wind_dir as double precision),
trim( s.weather),
trim( s.pod),
cast( s.vis as double precision),
cast( s.precip as double precision),
cast( s.pres as double precision),
cast( s.temp as double precision),
cast( s.dhi as double precision),
cast( s.dni as double precision),
cast( s.ghi as double precision),
cast( s.solar_rad as double precision),
cast( s.clouds as double precision)
from aws_s3_stage2_extracts.stage2_historicalweather s
inner join (SELECT substring(postcode, 1, length(postcode) - 3) as postcode
        FROM ref_cdb_addresses
        group by substring(postcode, 1, length(postcode) - 3)
        order by substring(postcode, 1, length(postcode) - 3)) addr on trim (addr.postcode) = trim (s.postcode)
where trim(s.postcode) = 'BN14' and left(s.timestamp_utc, 10) = '2019-03-17';


select
                                            trim( s.postcode),
--                                             trim( s.datetime),
--                                             cast( s.timestamp_local as timestamp),
                                            cast( s.timestamp_utc as timestamp), count(*)
                                        from aws_s3_stage2_extracts.stage2_historicalweather s
                                        inner join (SELECT substring(postcode, 1, length(postcode) - 3) as postcode
                                                    FROM ref_cdb_addresses
                                                    group by substring(postcode, 1, length(postcode) - 3)
                                                    order by substring(postcode, 1, length(postcode) - 3)) addr on trim(addr.postcode) = trim (s.postcode)
-- where trim(s.postcode) = 'BN14'
group by trim(s.postcode), cast( s.timestamp_utc as timestamp)
having count(*) > 1
;

SELECT substring(postcode, 1, length(postcode) - 3) as postcode
                                                    FROM ref_cdb_addresses
where substring(postcode, 1, length(postcode) - 3) = 'BN14'
                                                    group by substring(postcode, 1, length(postcode) - 3)
                                                    order by substring(postcode, 1, length(postcode) - 3)

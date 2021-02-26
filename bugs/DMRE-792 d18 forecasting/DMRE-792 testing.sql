select num_records, count(*)
from (select count(*) as num_records,
             gsp_group_id,
             pcl_id,
             pfl_id,
             ss_conf_id,
             time_pattern_regime
      from ref_d18_ppc_forecast
      group by gsp_group_id,
               pcl_id,
               pfl_id,
               ss_conf_id,
               time_pattern_regime) nr
group by num_records
order by num_records desc

select num_records, count(*)
from (select count(*) as num_records,
             gsp_group_id,
             pcl_id,
             pfl_id
      from ref_d18_bpp_forecast
      group by gsp_group_id,
               pcl_id,
               pfl_id) nr
group by num_records
order by num_records desc

select num_records, count(*)
from (select count(*) as num_records,
             gsp_group_id,
             pcl_id,
             pfl_id,
             ss_conf_id,
             time_pattern_regime
      from ref_d18_igloo_ppc_forecast
      group by gsp_group_id,
               pcl_id,
               pfl_id,
               ss_conf_id,
               time_pattern_regime) nr
group by num_records
order by num_records desc

select num_records, count(*)
from (select count(*) as num_records,
             gsp_group_id,
             pcl_id,
             pfl_id
      from ref_d18_igloo_bpp_forecast
      group by gsp_group_id,
               pcl_id,
               pfl_id) nr
group by num_records
order by num_records desc

--select * from ref_d18_ppc where gsp_group_id = '_H' and pcl_id = 8 and pfl_id = 1 and ss_conf_id = '0146' and time_pattern_regime = '00251'

-- Check no values exist in forecast tables where the same class & date exists in the "actual data" table of the same type
select count(*)
from ref_d18_ppc actual
         inner join ref_d18_ppc_forecast forecast
                    on actual.st_date = forecast.st_date and
                       actual.gsp_group_id = forecast.gsp_group_id and
                       actual.pcl_id = forecast.pcl_id and
                       actual.pfl_id = forecast.pfl_id and
                       actual.ss_conf_id = forecast.ss_conf_id and
                       actual.time_pattern_regime = forecast.time_pattern_regime

select count(*)
from ref_d18_bpp actual
         inner join ref_d18_bpp_forecast forecast
                    on actual.st_date = forecast.st_date and
                       actual.gsp_group_id = forecast.gsp_group_id and
                       actual.pcl_id = forecast.pcl_id and
                       actual.pfl_id = forecast.pfl_id

select count(*)
from ref_d18_igloo_ppc actual
         inner join ref_d18_igloo_ppc_forecast forecast
                    on actual.st_date = forecast.st_date and
                       actual.gsp_group_id = forecast.gsp_group_id and
                       actual.pcl_id = forecast.pcl_id and
                       actual.pfl_id = forecast.pfl_id and
                       actual.ss_conf_id = forecast.ss_conf_id and
                       actual.time_pattern_regime = forecast.time_pattern_regime

select count(*)
from ref_d18_igloo_bpp actual
         inner join ref_d18_igloo_bpp_forecast forecast
                    on actual.st_date = forecast.st_date and
                       actual.gsp_group_id = forecast.gsp_group_id and
                       actual.pcl_id = forecast.pcl_id and
                       actual.pfl_id = forecast.pfl_id

--Check there are no duplicates per-class on the same date within each table (e.g. from different run numbers)
select st_date,
       gsp_group_id,
       pcl_id,
       pfl_id,
       ss_conf_id,
       time_pattern_regime,
       count(*) as dups
from ref_d18_ppc_forecast
group by st_date, gsp_group_id, pcl_id, pfl_id, ss_conf_id, time_pattern_regime
having dups > 1;

select st_date,
       gsp_group_id,
       pcl_id,
       pfl_id,
       count(*) as dups
from ref_d18_bpp_forecast
group by st_date, gsp_group_id, pcl_id, pfl_id
having dups > 1

select st_date,
       gsp_group_id,
       pcl_id,
       pfl_id,
       ss_conf_id,
       time_pattern_regime,
       count(*) as dups
from ref_d18_igloo_ppc_forecast
group by st_date, gsp_group_id, pcl_id, pfl_id, ss_conf_id, time_pattern_regime
having dups > 1;

select st_date,
       gsp_group_id,
       pcl_id,
       pfl_id,
       count(*) as dups
from ref_d18_igloo_bpp_forecast
group by st_date, gsp_group_id, pcl_id, pfl_id
having dups > 1
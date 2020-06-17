drop table ref_cumulative_ppc;
create table ref_cumulative_ppc as
select trunc(st_date)                                            as ppc_date,
       gsp_group_id                                              as gsp,
       ss_conf_id                                                as ssc,
       cast(time_pattern_regime as bigint)                       as tpr,
       pcl_id                                                    as pcl,
       sum(ppc_sum)
       over (partition by gsp_group_id, ss_conf_id, time_pattern_regime::bigint, pcl_id
           order by st_date::timestamp rows unbounded preceding) as cumulative_ppc,
       count(ppc_sum)
       over (partition by gsp_group_id, ss_conf_id, time_pattern_regime::bigint, pcl_id
           order by st_date::timestamp rows unbounded preceding) as cumulative_ppc_count
from (select ppc_all.*
      from (select * from ref_d18_igloo_ppc union select * from ref_d18_igloo_ppc_forecast) ppc_all
               inner join (select distinct mpa_gsp.attributes_attributevalue as gsp,
                                           mpa_ssc.attributes_attributevalue as ssc,
                                           reg.registers_tpr::bigint         as tpr,
                                           mpa_pcl.attributes_attributevalue as pcl
                           from ref_meterpoints mp
                                    inner join ref_meters met on mp.account_id = met.account_id and
                                                                 mp.meter_point_id = met.meter_point_id and
                                                                 met.removeddate is null
                                    inner join ref_registers reg on met.account_id = reg.account_id and
                                                                    met.meter_point_id = reg.meter_point_id and
                                                                    met.meter_id = reg.meter_id
                                    inner join ref_meterpoints_attributes mpa_ssc
                                               on mp.account_id = mpa_ssc.account_id and
                                                  mp.meter_point_id = mpa_ssc.meter_point_id and
                                                  mpa_ssc.attributes_attributename = 'SSC'
                                    inner join ref_meterpoints_attributes mpa_gsp
                                               on mp.account_id = mpa_gsp.account_id and
                                                  mp.meter_point_id = mpa_gsp.meter_point_id and
                                                  mpa_gsp.attributes_attributename = 'GSP'
                                    inner join ref_meterpoints_attributes mpa_pcl
                                               on mp.account_id = mpa_pcl.account_id and
                                                  mp.meter_point_id = mpa_pcl.meter_point_id and
                                                  mpa_pcl.attributes_attributename = 'Profile Class'
                           where mp.meterpointtype = 'E') portfolio_register_types
                          on ppc_all.gsp_group_id = portfolio_register_types.gsp and
                             ppc_all.ss_conf_id = portfolio_register_types.ssc and
                             ppc_all.time_pattern_regime::bigint = portfolio_register_types.tpr and
                             ppc_all.pcl_id = portfolio_register_types.pcl) igl_ppc
order by st_date, gsp_group_id, ss_conf_id, time_pattern_regime::bigint, pcl_id;


select *
from ref_cumulative_ppc
where ppc_date = '2017-12-31'

drop table temp_ppc_filtered;
create table temp_ppc_filtered as
select ppc_all.*
from (select * from ref_d18_igloo_ppc union select * from ref_d18_igloo_ppc_forecast) ppc_all
         inner join (select distinct mpa_gsp.attributes_attributevalue as gsp,
                                     mpa_ssc.attributes_attributevalue as ssc,
                                     reg.registers_tpr::bigint         as tpr,
                                     mpa_pcl.attributes_attributevalue as pcl
                     from ref_meterpoints mp
                              inner join ref_meters met on mp.account_id = met.account_id and
                                                           mp.meter_point_id = met.meter_point_id and
                                                           met.removeddate is null
                              inner join ref_registers reg on met.account_id = reg.account_id and
                                                              met.meter_point_id = reg.meter_point_id and
                                                              met.meter_id = reg.meter_id
                              inner join ref_meterpoints_attributes mpa_ssc
                                         on mp.account_id = mpa_ssc.account_id and
                                            mp.meter_point_id = mpa_ssc.meter_point_id and
                                            mpa_ssc.attributes_attributename = 'SSC'
                              inner join ref_meterpoints_attributes mpa_gsp
                                         on mp.account_id = mpa_gsp.account_id and
                                            mp.meter_point_id = mpa_gsp.meter_point_id and
                                            mpa_gsp.attributes_attributename = 'GSP'
                              inner join ref_meterpoints_attributes mpa_pcl
                                         on mp.account_id = mpa_pcl.account_id and
                                            mp.meter_point_id = mpa_pcl.meter_point_id and
                                            mpa_pcl.attributes_attributename = 'Profile Class'
                     where mp.meterpointtype = 'E') portfolio_register_types
                    on ppc_all.gsp_group_id = portfolio_register_types.gsp and
                       ppc_all.ss_conf_id = portfolio_register_types.ssc and
                       ppc_all.time_pattern_regime::bigint = portfolio_register_types.tpr and
                       ppc_all.pcl_id = portfolio_register_types.pcl

select *
from temp_ppc_filtered
order by st_date


select distinct gsp_group_id, ss_conf_id, time_pattern_regime, pcl_id
from temp_ppc_filtered

select distinct attributes_attributename
from ref_meterpoints_attributes

select distinct metersattributes_attributename
from ref_meters_attributes

select count(*)
from ref_cumulative_ppc
where ppc_date = '2020-05-01'


/*
 Cumulative ALP / WCF / DAF / CV
 */

create table ref_cumulative_alp_cv as
truncate table ref_cumulative_alp_cv;
insert into ref_cumulative_alp_cv
select cwaalps.coeff_date,
       cwaalps.ldz,
       cwaalp,
       waalp_count,
       (select sum(raic.value / 2)
        from ref_alp_igloo_cv raic
        where raic.ldz = cwaalps.ldz
          and raic.applicable_for < cwaalps.coeff_date) as ccv,
       (select count(raic.value)
        from ref_alp_igloo_cv raic
        where raic.ldz = cwaalps.ldz
          and raic.applicable_for < cwaalps.coeff_date) as cv_count
from (select date                                                                                as coeff_date,
             ldz,
             (1 + (coalesce(alp.value * 0.5, 0) * (alp.variance))) * (alp.forecastdocumentation) as waalp,
             sum(waalp) over (partition by ldz order by coeff_date rows unbounded preceding)     as cwaalp,
             count(waalp) over (partition by ldz order by coeff_date rows unbounded preceding)   as waalp_count
      from ref_alp_igloo_daf_wcf alp) cwaalps
order by coeff_date, ldz

create table ref_aq_calc_params as
    truncate table ref_aq_calc_params;
insert into ref_aq_calc_params
select mp.account_id,
       mp.meter_point_id,
       met.meter_id,
       reg.register_id,
       mp.meterpointnumber,
       met.meterserialnumber                               as meter_serial,
       iia.gas_ldz                                         as ldz,
       iia.no_of_digits                                    as num_dials,
       nullif(iia.gas_imperial_meter_indicator, 'U') = 'Y' as imperial,
       iia.read_min_datetime_gas                           as first_read_date,
       iia.read_min_readings_gas                           as first_read_value,
       iia.igl_ind_aq                                      as prev_igl_ind_aq,
       iia.read_max_datetime_gas                           as prev_read_date,
       cwaalp.cwaalp                                       as today_cwaalp,
       cwaalp.waalp_count                                  as today_waalp_count,
       cwaalp.ccv                                          as today_ccv,
       cwaalp.cv_count                                     as today_cv_count,
       getdate()                                           as etlchange
from ref_meterpoints mp
         inner join ref_meters met on mp.account_id = met.account_id and
                                      mp.meter_point_id = met.meter_point_id and
                                      met.removeddate is null
         inner join ref_registers reg on reg.account_id = met.account_id and
                                         reg.meter_point_id = met.meter_point_id and
                                         reg.meter_id = met.meter_id and
                                         reg.registers_tprperioddescription is not null
         left join ref_calculated_igl_ind_aq iia on iia.account_id = mp.account_id and
                                                    iia.meterpoint_id = mp.meter_point_id and
                                                    iia.register_id = reg.register_id
         left join ref_cumulative_alp_cv cwaalp on cwaalp.ldz = iia.gas_ldz and
                                                   trunc(cwaalp.coeff_date) = trunc(getdate())
where nvl(least(mp.supplyenddate, mp.associationenddate), getdate() + 1) > getdate()
  and mp.meterpointtype = 'G'
order by account_id, meter_point_id, meter_id, register_id

select distinct gas_imperial_meter_indicator
from ref_calculated_igl_ind_aq
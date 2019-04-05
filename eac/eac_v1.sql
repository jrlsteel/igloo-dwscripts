-- drop table temp_ref_calculated_eac_v1;
delete from temp_ref_calculated_eac_v1;
-- create table temp_ref_calculated_eac_v1
--   DISTKEY(account_id) SORTKEY(account_id) as (
-- insert into temp_ref_calculated_eac_v1
-- select count(*) from (
select st.account_id,
       st.elec_GSP,
       st.elec_ssc,
       st.meterpoint_id,
       st.supplyend_date,
       st.meter_removed_date,
       st.register_id,
       st.no_of_digits,
       st.read_min_datetime_elec                                              as read_min_datetime_elec,
       st.read_max_datetime_elec                                              as read_max_datetime_elec,
       st.read_min_readings_elec                                              as read_min_readings_elec,
       st.read_max_readings_elec                                              as read_max_readings_elec,
       datediff(months, st.read_min_datetime_elec, st.read_max_datetime_elec) as read_months_diff_elec,
       coalesce(datediff(days, st.read_min_datetime_elec, st.read_max_datetime_elec), 0)   as read_days_diff_elec,
       st.ppc_count                                                           as no_of_ppc_rows,
       st.bpp_count                                                           as no_of_bpp_rows,
       st.read_consumption_elec,
       st.profile_class,
       st.tpr,
       st.ppc,
       st.bpp,
       st.smooth_param                                                        as sp,
       st.total_reads                                                         as total_reads,
       register_eac_elec                                                      as industry_eac_register,
       st.previous_eac                                                        as previous_ind_eac_estimates,
       st.latest_eac                                                          as latest_ind_eac_estimates,
       case when ppc is null or no_of_ppc_rows < datediff(days, st.read_min_datetime_elec, st.read_max_datetime_elec) then
            round(calculate_eac_v1(coalesce(st.smooth_param, 0), coalesce(st.bpp, 0), coalesce(st.read_consumption_elec, 0),
                               coalesce(st.previous_eac, 0)), 1)
       else
            round(calculate_eac_v1(coalesce(st.smooth_param, 0), coalesce(st.ppc, 0), coalesce(st.read_consumption_elec, 0),
                               coalesce(st.previous_eac, 0)), 1) end
        as igloo_eac_v1,
--        (case when ppc is null or no_of_ppc_rows < datediff(days, st.read_min_datetime_elec, st.read_max_datetime_elec) then
--             round(calculate_eac_v1(coalesce(st.smooth_param, 0), coalesce(st.bpp, 0), coalesce(st.read_consumption_elec, 0),
--                                coalesce(st.previous_eac, 0)), 1)
--        else
--               round(calculate_eac_v1(coalesce(st.smooth_param, 0), coalesce(st.ppc, 0), coalesce(st.read_consumption_elec, 0),
--                                coalesce(st.previous_eac, 0)), 1) end
--         )  - st.latest_eac                          as igloo_eac_v1_minus_industry_eac,
       getdate()                                                              as etlchange
from (select mp_elec.account_id                                                   as account_id,
             mp_elec.meter_point_id                                               as meterpoint_id,
             mp_elec.supplyenddate                                                as supplyend_date,
             mtrs_elec.removeddate                                                as meter_removed_date,
             reg_elec.register_id                                                 as register_id,
             rma_gsp.attributes_attributevalue                                    as elec_GSP,
             rma_ssc.attributes_attributevalue                                    as elec_ssc,
             max(read_valid.no_of_digits)                                         as no_of_digits,
             max(reg_elec.registers_eacaq)                                        as register_eac_elec,
             min(read_valid.meterreadingdatetime)                                 as read_min_datetime_elec,
             max(read_valid.meterreadingdatetime)                                 as read_max_datetime_elec,
             min(read_valid.corrected_reading)                                    as read_min_readings_elec,
             max(read_valid.corrected_reading)                                    as read_max_readings_elec,
             coalesce(max(read_valid.corrected_reading) - min(read_valid.corrected_reading), 0)as read_consumption_elec,
             rma_pcl.attributes_attributevalue                                    as profile_class,
             reg_elec.registers_tpr                                               as tpr,
             (select sum(ppc_sum)
              from ref_d18_igloo_ppc
              where gsp_group_id = rma_gsp.attributes_attributevalue
                and ss_conf_id = rma_ssc.attributes_attributevalue
                and cast(time_pattern_regime as bigint) = reg_elec.registers_tpr
                and pcl_id = cast(rma_pcl.attributes_attributevalue as integer)
                and st_date >= trunc(min(read_valid.meterreadingdatetime))
                and st_date < trunc(max(read_valid.meterreadingdatetime))
              group by gsp_group_id, ss_conf_id)                                  as ppc,
              (select count(*)
              from ref_d18_igloo_ppc
              where gsp_group_id = rma_gsp.attributes_attributevalue
                and ss_conf_id = rma_ssc.attributes_attributevalue
                and cast(time_pattern_regime as bigint) = reg_elec.registers_tpr
                and pcl_id = cast(rma_pcl.attributes_attributevalue as integer)
                and st_date >= trunc(min(read_valid.meterreadingdatetime))
                and st_date < trunc(max(read_valid.meterreadingdatetime))
              group by gsp_group_id)                                              as ppc_count,
              (select sum(bpp_sum)
              from ref_d18_igloo_bpp
              where gsp_group_id = rma_gsp.attributes_attributevalue
                and pcl_id = cast(rma_pcl.attributes_attributevalue as integer) and pfl_id = 1
                and st_date >= trunc(min(read_valid.meterreadingdatetime))
                and st_date < trunc(max(read_valid.meterreadingdatetime))
              group by gsp_group_id)                                              as bpp,
              (select count(*)
              from ref_d18_igloo_bpp
              where gsp_group_id = rma_gsp.attributes_attributevalue
                and pcl_id = cast(rma_pcl.attributes_attributevalue as integer) and pfl_id = 1
                and st_date >= trunc(min(read_valid.meterreadingdatetime))
                and st_date < trunc(max(read_valid.meterreadingdatetime))
              group by gsp_group_id)                                              as bpp_count,
             2                                                                    as smooth_param,
             read_valid.total_reads                                               as total_reads,
             coalesce(read_valid.previous_eac, 0)                                              as previous_eac,
             coalesce(read_valid.latest_eac, 0)                                                as latest_eac
      from ref_meterpoints mp_elec
             inner join ref_meterpoints_attributes rma_gsp on mp_elec.account_id = rma_gsp.account_id and mp_elec.meter_point_id = rma_gsp.meter_point_id and
                                                              rma_gsp.attributes_attributename = 'GSP'
             inner join ref_meterpoints_attributes rma_ssc on mp_elec.account_id = rma_ssc.account_id and mp_elec.meter_point_id = rma_ssc.meter_point_id and
                                                              rma_ssc.attributes_attributename = 'SSC'
             inner join ref_meterpoints_attributes rma_pcl on mp_elec.account_id = rma_pcl.account_id and mp_elec.meter_point_id = rma_pcl.meter_point_id and
                                                              rma_pcl.attributes_attributename = 'Profile Class'
             inner join ref_meters mtrs_elec
               on mtrs_elec.meter_point_id = mp_elec.meter_point_id and mtrs_elec.removeddate is NULL
             inner join ref_registers reg_elec on mtrs_elec.meter_id = reg_elec.meter_id
             left outer join (select max(
                                       case when y.n = 1 then estimation_value else 0 end) over (partition by y.register_id) latest_eac,
                                     max(
                                       case when y.n = 2 then estimation_value else 0 end) over (partition by y.register_id) previous_eac,
                                     y.account_id,
                                     y.meterpointnumber,
                                     y.registerreference,
                                     y.register_id,
                                     y.no_of_digits,
                                     y.meterreadingdatetime,
                                     y.meterreadingcreateddate,
                                     y.corrected_reading,
                                     y.total_reads
                              from (select r.*,
                                           dense_rank() over (partition by account_id, register_id order by meterreadingdatetime desc) n,
                                           count(*) over (partition by account_id, register_id)                                        total_reads
                                    from ref_readings_internal_valid r) y
                                     left outer join ref_estimates_elec_internal ee
                                       on ee.account_id = y.account_id and y.meterpointnumber = ee.mpan and
                                          y.registerreference = ee.register_id
                                            and y.meterserialnumber = ee.serial_number and
                                          ee.effective_from = y.meterreadingdatetime
                              where y.n <= 2
                                ) read_valid
               on read_valid.account_id = mp_elec.account_id and read_valid.register_id = reg_elec.register_id
             left outer join ref_account_status ac on ac.account_id = mp_elec.account_id

      where mp_elec.meterpointtype = 'E'
        and ac.account_id = 1831
        and (mp_elec.supplyenddate is null or mp_elec.supplyenddate > getdate())
        and upper(ac.status) = 'LIVE'
      group by mp_elec.account_id,
               mp_elec.meter_point_id,
               reg_elec.register_id,
               reg_elec.registers_tpr,
               mp_elec.supplyenddate,
               mtrs_elec.removeddate,
               rma_gsp.attributes_attributevalue,
               rma_ssc.attributes_attributevalue,
               rma_pcl.attributes_attributevalue,
               read_valid.total_reads,
               read_valid.previous_eac,
               read_valid.latest_eac,
               read_valid.register_id) st
--               )
--     st1
-- where st1.igloo_eac_v1_minus_industry_eac != 0 and st1.latest_ind_eac_estimates != 0 and st1.previous_ind_eac_estimates != 0;

/******* EAC_V1 on demand *********/
select st.account_id,
       st.elec_GSP,
       st.elec_ssc,
       st.meterpoint_id,
       st.supplyend_date,
       st.meter_removed_date,
       st.register_id,
       st.no_of_digits,
       st.read_min_datetime_elec                                              as read_min_datetime_elec,
       st.read_max_datetime_elec                                              as read_max_datetime_elec,
       st.read_min_readings_elec                                              as read_min_readings_elec,
       st.read_max_readings_elec                                              as read_max_readings_elec,
       datediff(months, st.read_min_datetime_elec, st.read_max_datetime_elec) as read_months_diff_elec,
       coalesce(datediff(days, st.read_min_datetime_elec, st.read_max_datetime_elec), 0)   as read_days_diff_elec,
       st.ppc_count                                                           as no_of_ppc_rows,
       st.read_consumption_elec,
       st.profile_class,
       st.tpr,
       st.ppc,
       st.bpp,
       st.smooth_param                                                        as sp,
       st.total_reads                                                         as total_reads,
       register_eac_elec                                                      as industry_eac_register,
       st.previous_eac                                                        as previous_ind_eac_estimates,
       st.latest_eac                                                          as latest_ind_eac_estimates,
       case when ppc is null or no_of_ppc_rows < datediff(days, st.read_min_datetime_elec, st.read_max_datetime_elec) then
            round(calculate_eac_v1(coalesce(st.smooth_param, 0), coalesce(st.bpp, 0), coalesce(st.read_consumption_elec, 0),
                               coalesce(st.previous_eac, 0)), 1)
       else
            round(calculate_eac_v1(coalesce(st.smooth_param, 0), coalesce(st.ppc, 0), coalesce(st.read_consumption_elec, 0),
                               coalesce(st.previous_eac, 0)), 1) end
        as igloo_eac_v1,
       getdate()                                                              as etlchange
from (select mp_elec.account_id                                                   as account_id,
             mp_elec.meter_point_id                                               as meterpoint_id,
             mp_elec.supplyenddate                                                as supplyend_date,
             mtrs_elec.removeddate                                                as meter_removed_date,
             reg_elec.register_id                                                 as register_id,
             rma_gsp.attributes_attributevalue                                    as elec_GSP,
             rma_ssc.attributes_attributevalue                                    as elec_ssc,
             max(read_valid.no_of_digits)                                         as no_of_digits,
             max(reg_elec.registers_eacaq)                                        as register_eac_elec,
             min(read_valid.meterreadingdatetime)                                 as read_min_datetime_elec,
             max(read_valid.meterreadingdatetime)                                 as read_max_datetime_elec,
             min(read_valid.corrected_reading)                                    as read_min_readings_elec,
             max(read_valid.corrected_reading)                                    as read_max_readings_elec,
             coalesce (max(read_valid.corrected_reading) - min(read_valid.corrected_reading), 0) as read_consumption_elec,
             rma_pcl.attributes_attributevalue                                    as profile_class,
             reg_elec.registers_tpr                                               as tpr,
             (select sum(ppc_sum)
              from ref_d18_igloo_ppc
              where gsp_group_id = rma_gsp.attributes_attributevalue
                and ss_conf_id = rma_ssc.attributes_attributevalue
                and cast(time_pattern_regime as bigint) = reg_elec.registers_tpr
                and pcl_id = cast(rma_pcl.attributes_attributevalue as integer)
                and st_date >= trunc(min(read_valid.meterreadingdatetime))
                and st_date < trunc(max(read_valid.meterreadingdatetime))
              group by gsp_group_id, ss_conf_id)                                  as ppc,
              (select count(*)
              from ref_d18_igloo_ppc
              where gsp_group_id = rma_gsp.attributes_attributevalue
                and ss_conf_id = rma_ssc.attributes_attributevalue
                and cast(time_pattern_regime as bigint) = reg_elec.registers_tpr
                and pcl_id = cast(rma_pcl.attributes_attributevalue as integer)
                and st_date >= trunc(min(read_valid.meterreadingdatetime))
                and st_date < trunc(max(read_valid.meterreadingdatetime))
              group by gsp_group_id)                                              as ppc_count,
              (select sum(bpp_sum)
              from ref_d18_igloo_bpp
              where gsp_group_id = rma_gsp.attributes_attributevalue
                and pcl_id = cast(rma_pcl.attributes_attributevalue as integer) and pfl_id = 1
                and st_date >= trunc(min(read_valid.meterreadingdatetime))
                and st_date < trunc(max(read_valid.meterreadingdatetime))
              group by gsp_group_id)                                              as bpp,
             2                                                                    as smooth_param,
             read_valid.total_reads                                               as total_reads,
             coalesce(read_valid.previous_eac, 0)                                as previous_eac,
             coalesce(read_valid.latest_eac, 0)                                   as latest_eac
      from ref_meterpoints mp_elec
             inner join ref_meterpoints_attributes rma_gsp on mp_elec.account_id = rma_gsp.account_id and mp_elec.meter_point_id = rma_gsp.meter_point_id and
                                                              rma_gsp.attributes_attributename = 'GSP'
             inner join ref_meterpoints_attributes rma_ssc on mp_elec.account_id = rma_ssc.account_id and mp_elec.meter_point_id = rma_ssc.meter_point_id and
                                                              rma_ssc.attributes_attributename = 'SSC'
             inner join ref_meterpoints_attributes rma_pcl on mp_elec.account_id = rma_pcl.account_id and mp_elec.meter_point_id = rma_pcl.meter_point_id and
                                                              rma_pcl.attributes_attributename = 'Profile Class'
             inner join ref_meters mtrs_elec
               on mtrs_elec.meter_point_id = mp_elec.meter_point_id and mtrs_elec.removeddate is NULL
             inner join ref_registers reg_elec on mtrs_elec.meter_id = reg_elec.meter_id
             left outer join (select max(
                                       case when y.n = 1 then estimation_value else 0 end) over (partition by y.register_id) latest_eac,
                                     max(
                                       case when y.n = 2 then estimation_value else 0 end) over (partition by y.register_id) previous_eac,
                                     y.account_id,
                                     y.meterpointnumber,
                                     y.registerreference,
                                     y.register_id,
                                     y.no_of_digits,
                                     y.meterreadingdatetime,
                                     y.meterreadingcreateddate,
                                     y.corrected_reading,
                                     y.total_reads
                              from (select r.*,
                                           dense_rank() over (partition by account_id, register_id order by meterreadingdatetime desc) n,
                                           count(*) over (partition by account_id, register_id)                                        total_reads
                                    from vw_corrected_round_clock_reading_pa r) y
                                     left outer join ref_estimates_elec_internal ee
                                       on ee.account_id = y.account_id and y.meterpointnumber = ee.mpan and
                                          y.registerreference = ee.register_id
                                            and y.meterserialnumber = ee.serial_number and
                                          ee.effective_from = y.meterreadingdatetime
                              where y.n <= 2
                                ) read_valid
               on read_valid.account_id = mp_elec.account_id and read_valid.register_id = reg_elec.register_id
             left outer join ref_account_status ac on ac.account_id = mp_elec.account_id

      where mp_elec.meterpointtype = 'E'
        and ac.account_id = {0}
        and (mp_elec.supplyenddate is null or mp_elec.supplyenddate > getdate())
        and upper(ac.status) = 'LIVE'
      group by mp_elec.account_id,
               mp_elec.meter_point_id,
               reg_elec.register_id,
               reg_elec.registers_tpr,
               mp_elec.supplyenddate,
               mtrs_elec.removeddate,
               rma_gsp.attributes_attributevalue,
               rma_ssc.attributes_attributevalue,
               rma_pcl.attributes_attributevalue,
               read_valid.total_reads,
               read_valid.previous_eac,
               read_valid.latest_eac,
               read_valid.register_id) st;


/******** EAC_v1 UDF ************/
create or replace function calculate_eac_v1(sp double precision, ppc double precision,
                                            consumption  double precision,
                                            previous_eac double precision)
  returns double precision
  stable
  language plpythonu
as $$
  import logging
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)

  aa = 0.0
  eac = 0.0
  b = sp * ppc
  if ppc != 0 and previous_eac != 0:
    aa = consumption / ppc
    if b > 1:
      eac = aa
    else:
      eac = (aa * b) + (previous_eac * (1 - b))
  return eac
$$;


/*** EAC_v1 analysis ***/

select
t1.category, t1.reason,
 count(*)
 from (
select t.*, case when (igloo_eac_v1 - latest_ind_eac_estimates = 0)
                      and (igloo_eac_v1 != 0 and igloo_eac_v1 is not null) then
                          'Exact match' else
                  case when (igloo_eac_v1 - latest_ind_eac_estimates != 0 and igloo_eac_v1 - latest_ind_eac_estimates is not null)
                      and (igloo_eac_v1 != 0 and igloo_eac_v1 is not null) then
                          'Not Exact match' else
                            'Igloo EAC Not calculated' end end as category,

            case when (igloo_eac_v1 - latest_ind_eac_estimates = 0)
                      and (igloo_eac_v1 != 0 and igloo_eac_v1 is not null) then
                          'Exact match' else
            case when (igloo_eac_v1 = 0 or igloo_eac_v1 is null) then
                case when (read_days_diff_elec = 0 or read_days_diff_elec is null) then
                        'Not Enough reads for calculation' else
                    case when (read_consumption_elec = 0 or read_consumption_elec is null) then
                        'Consumption is zero' else
                        case when (previous_ind_eac_estimates = 0 or previous_ind_eac_estimates is null) then
                            'Previous_EAC is not available for calculation' else
                            case when ((ppc is null or ppc = 0) and (bpp =0 or bpp is null)) then
                                'No PPC or BPP is available from d18 for calculation' end end end end else
            case when (igloo_eac_v1 - latest_ind_eac_estimates != 0 and igloo_eac_v1 - latest_ind_eac_estimates is not null) and (igloo_eac_v1 != 0 or igloo_eac_v1 is not null) then
                case when (latest_ind_eac_estimates = 0 or latest_ind_eac_estimates is null) then
                    'Latest EAC from Industry not available yet' else
                    case when (ppc is not null and no_of_ppc_rows < datediff(days, read_min_datetime_elec, read_max_datetime_elec)) or
                            (bpp is not null and no_of_bpp_rows < datediff(days, read_min_datetime_elec, read_max_datetime_elec)) then
                            'Only Partial ppc or bpp rows available' else
                        case when ((igloo_eac_v1 - latest_ind_eac_estimates between 1 and 10) or (igloo_eac_v1 - latest_ind_eac_estimates between -10 and -1)) then
                              'unknown(Within 10 units)' else
                            case when (igloo_eac_v1 - latest_ind_eac_estimates between 10 and 50 or igloo_eac_v1 - latest_ind_eac_estimates between -50 and -10) then
                                'unknown(Within 50 units)' else
                                      'unknown'
                                        end end end end end end end
                                            as reason,
       case when ppc is null or no_of_ppc_rows < datediff(days, read_min_datetime_elec, read_max_datetime_elec) then
          1 else 0
          end as bpp_used


from ref_calculated_eac_v1 t) t1
group by t1.category, t1.reason
order by category
-- inner join ref_account_status ac on ac.account_id = t1.account_id
-- left outer join ref_readings_internal_valid v on v.account_id = t1.account_id and v.register_id = t1.register_id

-- where upper(trim(ac.status)) = 'LIVE'
-- and t1.reason = 'Not Enough reads for calculation'

select * from ref_readings_internal_valid where account_id = 1854 ;
select * from ref_readings_internal where account_id = 14859 and register_id = 20903;

select * from ref_estimates_elec_internal where account_id = 1857;

select * from ref_meterpoints;


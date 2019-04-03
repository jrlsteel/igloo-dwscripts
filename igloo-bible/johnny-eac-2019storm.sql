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
       datediff(days, st.read_min_datetime_elec, st.read_max_datetime_elec)   as read_days_diff_elec,
       st.ppc_count                                                           as no_of_ppc_rows,
       st.read_consumption_elec,
       st.profile_class,
       st.tpr,
       st.ppc,
       st.smooth_param                                                        as sp,
       st.total_reads                                                         as total_reads,
       register_eac_elec                                                      as industry_eac_register,
       st.previous_eac                                                        as previous_ind_eac_estimates,
       st.latest_eac                                                          as latest_ind_eac_estimates,
       st.third_latest_eac                                                    as third_latest_ind_eac_estimates,
       calculate_eac_v1(coalesce(st.smooth_param, 0), coalesce(st.ppc, 0), coalesce(st.read_consumption_elec, 0),
                        coalesce(st.previous_eac, 0))                         as igloo_eac_v1,
       calculate_eac_v1(coalesce(st.smooth_param, 0), coalesce(st.ppc, 0), coalesce(st.read_consumption_elec, 0),
                        coalesce(third_latest_eac, 0))                        as igloo_eac_v1_third_latest_eac,
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
             max(read_valid.corrected_reading) - min(read_valid.corrected_reading)as read_consumption_elec,
             rma_pcl.attributes_attributevalue                                    as profile_class,
             reg_elec.registers_tpr                                               as tpr,
             (select sum(ppc_sum)
              from vw_ref_d18_ppc
              where gsp_group_id = rma_gsp.attributes_attributevalue
                and ss_conf_id = rma_ssc.attributes_attributevalue
                and cast(time_pattern_regime as bigint) = reg_elec.registers_tpr
                and pcl_id = rma_pcl.attributes_attributevalue
                and st_date > min(read_valid.meterreadingdatetime)
                and st_date <= max(read_valid.meterreadingdatetime)
              group by gsp_group_id, ss_conf_id)                                  as ppc,
             (select sum(ppc_sum)
              from vw_ref_d18_ppc
              where gsp_group_id = rma_gsp.attributes_attributevalue
                and ss_conf_id = rma_ssc.attributes_attributevalue
                and cast(time_pattern_regime as bigint) = reg_elec.registers_tpr
                and pcl_id = rma_pcl.attributes_attributevalue
                and st_date > min(read_valid.meterreadingdatetime)
                and st_date <= max(read_valid.meterreadingdatetime)
              group by gsp_group_id, ss_conf_id)                                  as previous_ppc,
             (select count(*)
              from vw_ref_d18_ppc
              where gsp_group_id = rma_gsp.attributes_attributevalue
                and ss_conf_id = rma_ssc.attributes_attributevalue
                and cast(time_pattern_regime as bigint) = reg_elec.registers_tpr
                and pcl_id = rma_pcl.attributes_attributevalue
                and st_date > min(read_valid.meterreadingdatetime)
                and st_date <= max(read_valid.meterreadingdatetime)
              group by gsp_group_id, ss_conf_id)                                  as ppc_count,
             2                                                                    as smooth_param,
             read_valid.total_reads                                               as total_reads,
             read_valid.previous_eac                                              as previous_eac,
             read_valid.latest_eac                                                as latest_eac,
             read_valid.third_lastest_eac                                         as third_latest_eac
      from ref_meterpoints mp_elec
             inner join ref_meterpoints_attributes rma_gsp on mp_elec.meter_point_id = rma_gsp.meter_point_id and
                                                              rma_gsp.attributes_attributename = 'GSP'
             inner join ref_meterpoints_attributes rma_ssc on mp_elec.meter_point_id = rma_ssc.meter_point_id and
                                                              rma_ssc.attributes_attributename = 'SSC'
             inner join ref_meterpoints_attributes rma_pcl on mp_elec.meter_point_id = rma_pcl.meter_point_id and
                                                              rma_pcl.attributes_attributename = 'Profile Class'
             inner join ref_meters mtrs_elec
               on mtrs_elec.meter_point_id = mp_elec.meter_point_id and mtrs_elec.removeddate is NULL
             inner join ref_registers reg_elec on mtrs_elec.meter_id = reg_elec.meter_id
             left outer join (select max(
                                       case when y.n = 1 then estimation_value else 0 end) over (partition by y.register_id) latest_eac,
                                     max(
                                       case when y.n = 2 then estimation_value else 0 end) over (partition by y.register_id) previous_eac,
                                     max(
                                       case when y.n = 3 then estimation_value else 0 end) over (partition by y.register_id) third_lastest_eac,
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
                              where y.n <= 3
                                and y.total_reads >= 4) read_valid
               on read_valid.account_id = mp_elec.account_id and read_valid.register_id = reg_elec.register_id
             left outer join ref_account_status ac on ac.account_id = mp_elec.account_id

      where mp_elec.meterpointtype = 'E'
        and (mp_elec.supplyenddate is null or mp_elec.supplyenddate > getdate())
        and upper(ac.status) = 'LIVE'
and mp_elec.account_id = 15187
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
               read_valid.third_lastest_eac,
               read_valid.register_id) st

select r.*,
       dense_rank() over (partition by account_id, register_id order by meterreadingdatetime desc) n,
       count(*) over (partition by account_id, register_id)                                        total_reads
from ref_readings_internal_valid r
where account_id = 15187

select * from (select r.*,
       dense_rank() over (partition by account_id, register_id order by meterreadingdatetime desc) n,
       count(*) over (partition by account_id, register_id)                                        total_reads
from ref_readings_internal_valid r) y left outer join ref_estimates_elec_internal ee
    on ee.account_id = y.account_id and y.meterpointnumber = ee.mpan and y.registerreference = ee.register_id
    and y.meterserialnumber = ee.serial_number and
    ee.effective_from = y.meterreadingdatetime
    where y.account_id = 15187


select * from ref_registers
where account_id= 15187

select * from ref_registers_audit
where account_id = 15187

select * from ref_meterpoints
where account_id = 1830


drop table temp_eac_js_analysis;
 create table temp_eac_js_analysis
   as
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
       datediff(days, st.read_min_datetime_elec, st.read_max_datetime_elec)   as read_days_diff_elec,
       st.ppc_count                                                           as no_of_ppc_rows,
       st.read_consumption_elec,
       st.profile_class,
       st.tpr,
       st.ppc,
       st.smooth_param                                                        as sp,
       st.total_reads                                                         as total_reads,
       register_eac_elec                                                      as industry_eac_register,
       st.previous_eac                                                        as previous_ind_eac_estimates,
       st.latest_eac                                                          as latest_ind_eac_estimates,
       calculate_eac_v1(coalesce(st.smooth_param, 0), coalesce(st.ppc, 0), coalesce(st.read_consumption_elec, 0),
                               coalesce(st.previous_eac, 0))                          as igloo_eac_v1,
 calculate_eac_v1(coalesce(st.smooth_param, 0), coalesce(st.ppc, 0), coalesce(st.read_consumption_elec, 0),
                               coalesce(st.previous_eac, 0))  -  st.latest_eac   as igloo_eac_v1_minus_industry_eac,
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
             max(read_valid.corrected_reading) - min(read_valid.corrected_reading)as read_consumption_elec,
             rma_pcl.attributes_attributevalue                                    as profile_class,
             reg_elec.registers_tpr                                               as tpr,
             (select sum(ppc_sum)
              from vw_ref_d18_ppc
              where gsp_group_id = rma_gsp.attributes_attributevalue
                and ss_conf_id = rma_ssc.attributes_attributevalue
                and cast(time_pattern_regime as bigint) = reg_elec.registers_tpr
                and pcl_id = rma_pcl.attributes_attributevalue
                and st_date >= min(read_valid.meterreadingdatetime)
                and st_date < max(read_valid.meterreadingdatetime)
              group by gsp_group_id, ss_conf_id)                                  as ppc,
             (select count(*)
              from vw_ref_d18_ppc
              where gsp_group_id = rma_gsp.attributes_attributevalue
                and ss_conf_id = rma_ssc.attributes_attributevalue
                and cast(time_pattern_regime as bigint) = reg_elec.registers_tpr
                and pcl_id = rma_pcl.attributes_attributevalue
                and st_date >= min(read_valid.meterreadingdatetime)
                and st_date < max(read_valid.meterreadingdatetime)
              group by gsp_group_id, ss_conf_id)                                  as ppc_count,
             2                                                                    as smooth_param,
             read_valid.total_reads                                               as total_reads,
             read_valid.previous_eac                                              as previous_eac,
             read_valid.latest_eac                                                as latest_eac
      from ref_meterpoints mp_elec
             inner join ref_meterpoints_attributes rma_gsp on mp_elec.meter_point_id = rma_gsp.meter_point_id and
                                                              rma_gsp.attributes_attributename = 'GSP'
             inner join ref_meterpoints_attributes rma_ssc on mp_elec.meter_point_id = rma_ssc.meter_point_id and
                                                              rma_ssc.attributes_attributename = 'SSC'
             inner join ref_meterpoints_attributes rma_pcl on mp_elec.meter_point_id = rma_pcl.meter_point_id and
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
                                and y.total_reads >= 4) read_valid
               on read_valid.account_id = mp_elec.account_id and read_valid.register_id = reg_elec.register_id
             left outer join ref_account_status ac on ac.account_id = mp_elec.account_id

      where mp_elec.meterpointtype = 'E'
      --and mp_elec.account_id = 15187
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

select count(*) from temp_eac_js_analysis
where igloo_eac_v1_minus_industry_eac > 1

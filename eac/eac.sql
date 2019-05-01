
/**** PA EAC sql ****/
select * from (
select st.account_id,
                       st.elec_GSP,
                       st.elec_ssc,
                       st.meterpoint_id,
                       st.supplyend_date,
                       st.meter_removed_date,
                       st.register_id,
                       st.no_of_digits,
                       st.read_min_created_date_elec                                                  as read_min_created_date_elec,
                       st.read_max_created_date_elec                                                  as read_max_created_date_elec,
                       st.read_min_readings_elec                                                      as read_min_readings_elec,
                       st.read_max_readings_elec                                                      as read_max_readings_elec,
                       datediff(months, st.read_min_created_date_elec, st.read_max_created_date_elec) as read_months_diff_elec,
                       datediff(days, st.read_min_created_date_elec, st.read_max_created_date_elec)   as read_days_diff_elec,
                       ppc_count                                                                      as ppc_count,
                       profile_class                                                                  as profile_class,
                       st.read_consumption_elec,
                       st.ppc,
                       st.bpp,
                       st.tpr,
                       register_eac_elec                                                              as industry_eac,
                       case when (st.ppc is not null and st.ppc != 0
                          and st.ppc_count = datediff(days, st.read_min_created_date_elec, st.read_max_created_date_elec))
                          then
                                ((1 / st.ppc) * st.read_consumption_elec)
                          else
                          case when st.bpp is not null and st.bpp != 0 and st.ppc_count <= datediff(days, st.read_min_created_date_elec, st.read_max_created_date_elec) then
                                ((1 / st.bpp) * st.read_consumption_elec)
                          else 0
                        end end                                                                    as igloo_eac,
                       getdate()                                                                   as etlchange
                from (select mp_elec.account_id                                                    as account_id,
                             mp_elec.meter_point_id                                                as meterpoint_id,
                             mp_elec.supplyenddate                                                 as supplyend_date,
                             mtrs_elec.removeddate                                                 as meter_removed_date,
                             reg_elec.register_id                                                  as register_id,
                             rma_gsp.attributes_attributevalue                                     as elec_GSP,
                             rma_ssc.attributes_attributevalue                                     as elec_ssc,
                             max(read_valid.no_of_digits)                                          as no_of_digits,
                             max(reg_elec.registers_eacaq)                                         as register_eac_elec,
                             min(read_valid.meterreadingdatetime)                                  as read_min_created_date_elec,
                             max(read_valid.meterreadingdatetime)                                  as read_max_created_date_elec,
                             min(read_valid.corrected_reading)                                     as read_min_readings_elec,
                             max(read_valid.corrected_reading)                                     as read_max_readings_elec,
                             max(read_valid.corrected_reading) - min(read_valid.corrected_reading) as read_consumption_elec,
                             rma_pcl.attributes_attributevalue                                     as profile_class,
                             reg_elec.registers_tpr                                                as tpr,
                             (select sum(ppc_sum)
                              from ref_d18_igloo_ppc
                              where gsp_group_id = rma_gsp.attributes_attributevalue
                                and ss_conf_id = rma_ssc.attributes_attributevalue
                                and cast (time_pattern_regime as bigint) = reg_elec.registers_tpr
                                and pcl_id = cast(rma_pcl.attributes_attributevalue as integer)
                               and st_date >= trunc(min(read_valid.meterreadingdatetime))
                                and st_date < trunc(max(read_valid.meterreadingdatetime))
                              group by gsp_group_id, ss_conf_id)                                      ppc,
                             (select sum(bpp_sum)
                              from ref_d18_igloo_bpp
                              where gsp_group_id = rma_gsp.attributes_attributevalue
                                and pcl_id = cast(rma_pcl.attributes_attributevalue as integer) and pfl_id = 1
                                and st_date >= trunc(min(read_valid.meterreadingdatetime))
                                and st_date < trunc(max(read_valid.meterreadingdatetime))
                              group by gsp_group_id)                                              as bpp,
                              (select count(*)
                              from ref_d18_igloo_ppc
                              where gsp_group_id = rma_gsp.attributes_attributevalue
                                and ss_conf_id = rma_ssc.attributes_attributevalue
                                and cast (time_pattern_regime as bigint) = reg_elec.registers_tpr
                                and pcl_id = cast(rma_pcl.attributes_attributevalue as integer)
                                 and st_date >= trunc(min(read_valid.meterreadingdatetime))
                                and st_date < trunc(max(read_valid.meterreadingdatetime))
                              group by gsp_group_id, ss_conf_id)                                      ppc_count
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
                             inner join ref_readings_internal_valid read_valid on reg_elec.register_id = read_valid.register_id
                      where
                      mp_elec.account_id = 2596 and
                      reg_elec.register_id = 3241 and
                          mp_elec.meterpointtype = 'E'
                        and (mp_elec.supplyenddate is null or mp_elec.supplyenddate > getdate())
                      group by mp_elec.account_id,
                               mp_elec.meter_point_id,
                               reg_elec.register_id,
                               reg_elec.registers_tpr,
                               mp_elec.supplyenddate,
                               mtrs_elec.removeddate,
                               rma_gsp.attributes_attributevalue,
                               rma_ssc.attributes_attributevalue,
                               rma_pcl.attributes_attributevalue)
              st) st1


select st.account_id,
             st.elec_GSP,
             st.elec_ssc,
             st.meterpoint_id,
             st.supplyend_date,
             st.meter_removed_date,
             st.register_id,
             st.no_of_digits,
             st.read_min_created_date_elec as read_min_created_date_elec,
             st.read_max_created_date_elec as read_max_created_date_elec,
             st.read_min_readings_elec as read_min_readings_elec,
             st.read_max_readings_elec as read_max_readings_elec,
             datediff(months, st.read_min_created_date_elec, st.read_max_created_date_elec) as read_months_diff_elec,
             datediff(days, st.read_min_created_date_elec, st.read_max_created_date_elec) as read_days_diff_elec,
             st.read_consumption_elec,
             ppc,
             register_eac_elec as industry_eac,
             case
               when ppc != 0 then ((1 / ppc) * st.read_consumption_elec)
               else 0 end          as igloo_eac
      from (
           select mp_elec.account_id                            as account_id,
                   mp_elec.meter_point_id                     as meterpoint_id,
                   mp_elec.supplyenddate                      as supplyend_date,
                   mtrs_elec.removeddate                      as meter_removed_date,
                   reg_elec.register_id                       as register_id,
                   max(read_valid.no_of_digits)               as no_of_digits,
                   rma_gsp.attributes_attributevalue          as elec_GSP,
                   rma_ssc.attributes_attributevalue          as elec_ssc,
                   max(reg_elec.registers_eacaq)              as register_eac_elec,
                   min(read_valid.meterreadingdatetime) as read_min_created_date_elec,
                   max(read_valid.meterreadingdatetime) as read_max_created_date_elec,
                   min(read_valid.corrected_reading) as read_min_readings_elec,
                   max(read_valid.corrected_reading) as read_max_readings_elec,
                   max(read_valid.corrected_reading) -
                   min(read_valid.corrected_reading) as read_consumption_elec,
                   (select sum(ppc_sum)
                    from ref_d18_igloo_ppc
                    where gsp_group_id =  rma_gsp.attributes_attributevalue
                      and ss_conf_id = rma_ssc.attributes_attributevalue
                      and cast (time_pattern_regime as bigint) = reg_elec.registers_tpr
                      and pcl_id = cast(rma_pcl.attributes_attributevalue as integer)
                      and st_date between min(read_valid.meterreadingdatetime) and max(read_valid.meterreadingdatetime)
                    group by gsp_group_id, ss_conf_id) ppc
            from ref_meterpoints mp_elec
             inner join ref_meterpoints_attributes rma_gsp on mp_elec.account_id = rma_gsp.account_id and mp_elec.meter_point_id = rma_gsp.meter_point_id and
                                                              rma_gsp.attributes_attributename = 'GSP'
             inner join ref_meterpoints_attributes rma_ssc on mp_elec.account_id = rma_ssc.account_id and mp_elec.meter_point_id = rma_ssc.meter_point_id and
                                                              rma_ssc.attributes_attributename = 'SSC'
             inner join ref_meterpoints_attributes rma_pcl on mp_elec.account_id = rma_pcl.account_id and mp_elec.meter_point_id = rma_pcl.meter_point_id and
                                                              rma_pcl.attributes_attributename = 'Profile Class'
                   inner join ref_meters mtrs_elec on mtrs_elec.meter_point_id = mp_elec.meter_point_id and mtrs_elec.removeddate is NULL
                   inner join ref_registers reg_elec on mtrs_elec.meter_id = reg_elec.meter_id
                   left outer join ref_readings_internal_valid_bak_26042019 read_valid
                     on read_valid.register_id = reg_elec.register_id
            where
                  mp_elec.account_id in
                  (
                    1856,
                    1858,
                    1904,
                    1933,
                    1977
) and
                  mp_elec.meterpointtype = 'E'
--                   (mp_elec.supplyenddate is null or mp_elec.supplyenddate > getdate())
      group by mp_elec.account_id,
               mp_elec.meter_point_id,
               reg_elec.register_id,
                reg_elec.registers_tpr,
                mp_elec.supplyenddate,
                mtrs_elec.removeddate,
               rma_gsp.attributes_attributevalue,
               rma_ssc.attributes_attributevalue,
               rma_pcl.attributes_attributevalue
               ) st

select * from ref_meterpoints_attributes where

attributes_attributevalue != 1 and attributes_attributename = 'Profile Class'

select * from ref_readings_internal_valid_bak_26042019 where account_id = 36723
and meterpointtype = 'E';


select * from ref_meterpoints where account_id = 36723;
select * from ref_meterpoints_audit where account_id = 1872;
select * from ref_calculated_eac where account_id = 1872;
select * from ref_account_status where account_id = 30465;

select * from ref_meterpoints mp
inner join ref_account_status_audit au on au.account_id = mp.account_id
where status in ('Lost')
order by etlchange;


select *
from ref_readings_internal_valid_bak_26042019
where
-- meter_rolled_over = 'Y'
--   and no_of_digits != 0
--   and max_previous_reading < 99999 - 10000 and
    			register_id = 2069 and
    		 account_id = 14805 and
    			meterpointtype = 'E'
order by register_id, meterreadingdatetime;

select * from ref_readings_internal_valid_bak_26042019 where account_id = 1872;

select st.account_id,
                       st.elec_GSP,
                       st.elec_ssc,
                       st.meterpoint_id,
                       st.supplyend_date,
                       st.meter_removed_date,
                       st.register_id,
                       st.no_of_digits,
                       st.read_min_created_date_elec                                                  as read_min_created_date_elec,
                       st.read_max_created_date_elec                                                  as read_max_created_date_elec,
                       st.read_min_readings_elec                                                      as read_min_readings_elec,
                       st.read_max_readings_elec                                                      as read_max_readings_elec,
                       datediff(months, st.read_min_created_date_elec, st.read_max_created_date_elec) as read_months_diff_elec,
                       datediff(days, st.read_min_created_date_elec, st.read_max_created_date_elec)   as read_days_diff_elec,
                       st.read_consumption_elec,
                       ppc,
                       register_eac_elec                                                              as industry_eac,
                       case
                         when ppc != 0 then ((1 / ppc) * st.read_consumption_elec)
                         else 0 end                                                                   as igloo_eac,
                       getdate()                                                                      as etlchange
                from (select mp_elec.account_id                                                    as account_id,
                             mp_elec.meter_point_id                                                as meterpoint_id,
                             mp_elec.supplyenddate                                                 as supplyend_date,
                             mtrs_elec.removeddate                                                 as meter_removed_date,
                             reg_elec.register_id                                                  as register_id,
                             rma_gsp.attributes_attributevalue                                     as elec_GSP,
                             rma_ssc.attributes_attributevalue                                     as elec_ssc,
                             max(read_valid.no_of_digits)                                          as no_of_digits,
                             max(reg_elec.registers_eacaq)                                         as register_eac_elec,
                             min(read_valid.meterreadingdatetime)                                  as read_min_created_date_elec,
                             max(read_valid.meterreadingdatetime)                                  as read_max_created_date_elec,
                             min(read_valid.corrected_reading)                                     as read_min_readings_elec,
                             max(read_valid.corrected_reading)                                     as read_max_readings_elec,
                             max(read_valid.corrected_reading) - min(read_valid.corrected_reading) as read_consumption_elec,
                             rma_pcl.attributes_attributevalue                                    as profile_class,
                             (select sum(ppc_sum)
                              from ref_d18_igloo_ppc_bak_26042019
                              where gsp_group_id = rma_gsp.attributes_attributevalue
                                and ss_conf_id = rma_ssc.attributes_attributevalue
                                and cast (time_pattern_regime as bigint) = reg_elec.registers_tpr
                                and pcl_id = cast(rma_pcl.attributes_attributevalue as integer)
                                and st_date between min(read_valid.meterreadingdatetime) and max(read_valid.meterreadingdatetime)
                              group by gsp_group_id, ss_conf_id)                                      ppc
                      from ref_meterpoints mp_elec
                             inner join ref_meterpoints_attributes rma_gsp on mp_elec.account_id = rma_gsp.account_id
                             and mp_elec.meter_point_id = rma_gsp.meter_point_id and rma_gsp.attributes_attributename = 'GSP'
                             inner join ref_meterpoints_attributes rma_ssc on mp_elec.account_id = rma_ssc.account_id
                             and mp_elec.meter_point_id = rma_ssc.meter_point_id and rma_ssc.attributes_attributename = 'SSC'
                             inner join ref_meterpoints_attributes rma_pcl on mp_elec.account_id = rma_pcl.account_id
                             and mp_elec.meter_point_id = rma_pcl.meter_point_id and rma_pcl.attributes_attributename = 'Profile Class'
                             inner join ref_meters mtrs_elec on mp_elec.account_id = mtrs_elec.account_id
                             and mtrs_elec.meter_point_id = mp_elec.meter_point_id and mtrs_elec.removeddate is NULL
                             inner join ref_registers reg_elec on mp_elec.account_id = reg_elec.account_id and mtrs_elec.meter_id = reg_elec.meter_id
                             inner join ref_readings_internal_valid_bak_26042019 read_valid on mp_elec.account_id = read_valid.account_id
                             and reg_elec.register_id = read_valid.register_id
                      where
                          mp_elec.meterpointtype = 'E'
                        and (mp_elec.supplyenddate is null or mp_elec.supplyenddate > getdate())
                        and mp_elec.account_id = 36723
                      group by mp_elec.account_id,
                               mp_elec.meter_point_id,
                               reg_elec.register_id,
                               reg_elec.registers_tpr,
                               mp_elec.supplyenddate,
                               mtrs_elec.removeddate,
                               rma_gsp.attributes_attributevalue,
                               rma_ssc.attributes_attributevalue,
                               rma_pcl.attributes_attributevalue) st
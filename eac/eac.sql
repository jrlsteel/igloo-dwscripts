
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
--                       mp_elec.account_id = 2596 and
--                       reg_elec.register_id = 3241 and
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


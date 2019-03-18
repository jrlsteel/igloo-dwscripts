create view vw_igloo_daily_and_hourly_calculated_reads
as
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
       register_eac_elec                                                              as industry_eac,
       d18.time_pattern_regime                                                        as tpr,
       d18.st_date                                                                    as d18_st_date,
       sum(ppc_sum) over (partition by account_id)                                    as ppc_sum,
       case
         when ppc_sum != 0 then ((1 / sum(ppc_sum) over (partition by account_id)) * st.read_consumption_elec)
         else 0 end                                                                   as igloo_eac,
         (ppc_sum * st.read_consumption_elec) as igloo_edc,
         (ppc_1 * st.read_consumption_elec) as igloo_edhc_1,
         (ppc_2 * st.read_consumption_elec) as igloo_edhc_2,
         (ppc_3 * st.read_consumption_elec) as igloo_edhc_3,
         (ppc_4 * st.read_consumption_elec) as igloo_edhc_4,
         (ppc_5 * st.read_consumption_elec) as igloo_edhc_5,
         (ppc_6 * st.read_consumption_elec) as igloo_edhc_6,
         (ppc_7 * st.read_consumption_elec) as igloo_edhc_7,
         (ppc_8 * st.read_consumption_elec) as igloo_edhc_8,
         (ppc_9 * st.read_consumption_elec) as igloo_edhc_9,
         (ppc_10 * st.read_consumption_elec) as igloo_edhc_10,
         (ppc_11 * st.read_consumption_elec) as igloo_edhc_11,
         (ppc_12 * st.read_consumption_elec) as igloo_edhc_12,
         (ppc_13 * st.read_consumption_elec) as igloo_edhc_13,
         (ppc_14 * st.read_consumption_elec) as igloo_edhc_14,
         (ppc_15 * st.read_consumption_elec) as igloo_edhc_15,
         (ppc_16 * st.read_consumption_elec) as igloo_edhc_16,
         (ppc_17 * st.read_consumption_elec) as igloo_edhc_17,
         (ppc_18 * st.read_consumption_elec) as igloo_edhc_18,
         (ppc_19 * st.read_consumption_elec) as igloo_edhc_19,
         (ppc_20 * st.read_consumption_elec) as igloo_edhc_20,
         (ppc_21 * st.read_consumption_elec) as igloo_edhc_21,
         (ppc_22 * st.read_consumption_elec) as igloo_edhc_22,
         (ppc_23 * st.read_consumption_elec) as igloo_edhc_23,
         (ppc_24 * st.read_consumption_elec) as igloo_edhc_24,
         (ppc_25 * st.read_consumption_elec) as igloo_edhc_25,
         (ppc_26 * st.read_consumption_elec) as igloo_edhc_26,
         (ppc_27 * st.read_consumption_elec) as igloo_edhc_27,
         (ppc_28 * st.read_consumption_elec) as igloo_edhc_28,
         (ppc_29 * st.read_consumption_elec) as igloo_edhc_29,
         (ppc_30 * st.read_consumption_elec) as igloo_edhc_30,
         (ppc_31 * st.read_consumption_elec) as igloo_edhc_31,
         (ppc_32 * st.read_consumption_elec) as igloo_edhc_32,
         (ppc_33 * st.read_consumption_elec) as igloo_edhc_33,
         (ppc_34 * st.read_consumption_elec) as igloo_edhc_34,
         (ppc_35 * st.read_consumption_elec) as igloo_edhc_35,
         (ppc_36 * st.read_consumption_elec) as igloo_edhc_36,
         (ppc_37 * st.read_consumption_elec) as igloo_edhc_37,
         (ppc_38 * st.read_consumption_elec) as igloo_edhc_38,
         (ppc_39 * st.read_consumption_elec) as igloo_edhc_39,
         (ppc_40 * st.read_consumption_elec) as igloo_edhc_40,
         (ppc_41 * st.read_consumption_elec) as igloo_edhc_41,
         (ppc_42 * st.read_consumption_elec) as igloo_edhc_42,
         (ppc_43 * st.read_consumption_elec) as igloo_edhc_43,
         (ppc_44 * st.read_consumption_elec) as igloo_edhc_44,
         (ppc_45 * st.read_consumption_elec) as igloo_edhc_45,
         (ppc_46 * st.read_consumption_elec) as igloo_edhc_46,
         (ppc_47 * st.read_consumption_elec) as igloo_edhc_47,
         (ppc_48 * st.read_consumption_elec) as igloo_edhc_48,
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
             max(reg_elec.registers_tpr)                                           as tpr
      from ref_meterpoints mp_elec
             inner join ref_meterpoints_attributes rma_gsp on mp_elec.meter_point_id = rma_gsp.meter_point_id and
                                                              rma_gsp.attributes_attributename = 'GSP'
             inner join ref_meterpoints_attributes rma_ssc on mp_elec.meter_point_id = rma_ssc.meter_point_id and
                                                              rma_ssc.attributes_attributename = 'SSC'
             inner join ref_meters mtrs_elec
               on mtrs_elec.meter_point_id = mp_elec.meter_point_id and mtrs_elec.removeddate is NULL
             inner join ref_registers reg_elec on mtrs_elec.meter_id = reg_elec.meter_id
             inner join ref_readings_internal_valid read_valid on reg_elec.register_id = read_valid.register_id
      where mp_elec.account_id = 1831
        and mp_elec.meterpointtype = 'E'
        and (mp_elec.supplyenddate is null or mp_elec.supplyenddate > getdate())
      group by mp_elec.account_id,
               mp_elec.meter_point_id,
               reg_elec.register_id,
               reg_elec.registers_tpr,
               mp_elec.supplyenddate,
               mtrs_elec.removeddate,
               rma_gsp.attributes_attributevalue,
               rma_ssc.attributes_attributevalue) st
       inner join vw_ref_d18_ppc d18 on d18.gsp_group_id = st.elec_GSP
                                          and d18.pcl_id = 1
                                          and d18.ss_conf_id = st.elec_ssc
                                          and cast(d18.time_pattern_regime as bigint) = st.tpr
                                          and
                                        d18.st_date between st.read_min_created_date_elec and st.read_max_created_date_elec
order by d18.st_date;
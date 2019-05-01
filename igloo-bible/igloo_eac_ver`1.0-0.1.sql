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
--       st.read_min_datetime_elec_created,
--       st.read_max_datetime_elec_created,
       datediff(hours, st.read_min_datetime_elec, st.read_max_datetime_elec)  as read_days_diff_elec,
--        datediff(hours , st.read_min_datetime_elec_created, st.read_max_datetime_elec_created)     as read_days_diff_elec_created,

       st.read_consumption_elec,
       ppc,
--        case
--          when ppc != 0 then ((1 / ppc) * st.read_consumption_elec)
--          else 0 end                                                                     as igloo_eac,
       st.total_reads                                                         as total_reads,
       st.rv_register_id                                                      as rv_register_id,
       register_eac_elec                                                      as industry_eac_register,
       st.previous_eac                                                        as previous_ind_eac_estimates,
       st.latest_eac                                                          as latest_ind_eac_estimates,
       case
         when (ppc != 0 or ppc is not null) and st.previous_eac != 0
                 then ((2 * ppc) * (st.read_consumption_elec / ppc) + (1 - (2 * ppc)) * st.previous_eac)
         else 0 end                                                           as igloo_eac_v1,
       case
         when (ppc != 0 or ppc is not null) and st.previous_eac != 0
                 then st.latest_eac - ((2 * ppc) * (st.read_consumption_elec / ppc) + (1 - (2 * ppc)) * st.previous_eac)
         else 0 end                                                              ind_minus_igloo_eac,
       case
         when st.latest_eac != 0 and (ppc != 0 or ppc is not null) and st.previous_eac != 0
                 then (st.latest_eac /
                       ((2 * ppc) * (st.read_consumption_elec / ppc) + (1 - (2 * ppc)) * st.previous_eac))
         else 0 end                                                              ind_igloo_perc,
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
--              min(read_valid.meterreadingcreateddate)                                 as read_min_datetime_elec_created,
--              max(read_valid.meterreadingcreateddate)                                 as read_max_datetime_elec_created,

             min(read_valid.corrected_reading)                                    as read_min_readings_elec,
             max(read_valid.corrected_reading)                                    as read_max_readings_elec,
             max(read_valid.corrected_reading) - min(read_valid.corrected_reading)as read_consumption_elec,
             (select sum(ppc_sum)
              from ref_d18_igloo_ppc
              where gsp_group_id = rma_gsp.attributes_attributevalue
                and ss_conf_id = rma_ssc.attributes_attributevalue
                and cast(time_pattern_regime as bigint) = reg_elec.registers_tpr
                and pcl_id = 1
                and st_date > min(read_valid.meterreadingdatetime)
                and st_date <= max(read_valid.meterreadingdatetime)
              group by gsp_group_id, ss_conf_id)                                     ppc,
             read_valid.register_id                                               as rv_register_id,
             read_valid.total_reads                                               as total_reads,
             2                                                                    as smooth_factor,
             read_valid.previous_eac                                              as previous_eac,
             read_valid.latest_eac                                                as latest_eac
      from ref_meterpoints mp_elec
             inner join ref_meterpoints_attributes rma_gsp on mp_elec.meter_point_id = rma_gsp.meter_point_id and
                                                              rma_gsp.attributes_attributename = 'GSP'
             inner join ref_meterpoints_attributes rma_ssc on mp_elec.meter_point_id = rma_ssc.meter_point_id and
                                                              rma_ssc.attributes_attributename = 'SSC'
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
                                and y.total_reads >= 4) read_valid on read_valid.register_id = reg_elec.register_id

      where
        mp_elec.account_id = 4601 and
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
               read_valid.total_reads,
               read_valid.previous_eac,
               read_valid.latest_eac,
               read_valid.register_id) st

         select (((((((((((((((((((((((((((((((((((((((((((((((( COALESCE(ref_d18_ppc.ppc_2, (0) :: double precision) +
                                                         COALESCE(ref_d18_ppc.ppc_3, (0) :: double precision)) +
                                                        COALESCE(ref_d18_ppc.ppc_4, (0) :: double precision)) +
                                                       COALESCE(ref_d18_ppc.ppc_5, (0) :: double precision)) +
                                                      COALESCE(ref_d18_ppc.ppc_6, (0) :: double precision)) +
                                                     COALESCE(ref_d18_ppc.ppc_7, (0) :: double precision)) +
                                                    COALESCE(ref_d18_ppc.ppc_8, (0) :: double precision)) +
                                                   COALESCE(ref_d18_ppc.ppc_9, (0) :: double precision)) +
                                                  COALESCE(ref_d18_ppc.ppc_10, (0) :: double precision)) +
                                                 COALESCE(ref_d18_ppc.ppc_11, (0) :: double precision)) +
                                                COALESCE(ref_d18_ppc.ppc_12, (0) :: double precision)) +
                                               COALESCE(ref_d18_ppc.ppc_13, (0) :: double precision)) +
                                              COALESCE(ref_d18_ppc.ppc_14, (0) :: double precision)) +
                                             COALESCE(ref_d18_ppc.ppc_15, (0) :: double precision)) +
                                            COALESCE(ref_d18_ppc.ppc_16, (0) :: double precision)) +
                                           COALESCE(ref_d18_ppc.ppc_17, (0) :: double precision)) +
                                          COALESCE(ref_d18_ppc.ppc_18, (0) :: double precision)) +
                                         COALESCE(ref_d18_ppc.ppc_19, (0) :: double precision)) +
                                        COALESCE(ref_d18_ppc.ppc_20, (0) :: double precision)) +
                                       COALESCE(ref_d18_ppc.ppc_21, (0) :: double precision)) +
                                      COALESCE(ref_d18_ppc.ppc_22, (0) :: double precision)) +
                                     COALESCE(ref_d18_ppc.ppc_23, (0) :: double precision)) +
                                    COALESCE(ref_d18_ppc.ppc_24, (0) :: double precision)) +
                                   COALESCE(ref_d18_ppc.ppc_25, (0) :: double precision)) +
                                  COALESCE(ref_d18_ppc.ppc_26, (0) :: double precision)) +
                                 COALESCE(ref_d18_ppc.ppc_27, (0) :: double precision)) +
                                COALESCE(ref_d18_ppc.ppc_28, (0) :: double precision)) +
                               COALESCE(ref_d18_ppc.ppc_29, (0) :: double precision)) +
                              COALESCE(ref_d18_ppc.ppc_30, (0) :: double precision)) +
                             COALESCE(ref_d18_ppc.ppc_31, (0) :: double precision)) +
                            COALESCE(ref_d18_ppc.ppc_32, (0) :: double precision)) +
                           COALESCE(ref_d18_ppc.ppc_33, (0) :: double precision)) +
                          COALESCE(ref_d18_ppc.ppc_34, (0) :: double precision)) +
                         (+COALESCE(ref_d18_ppc.ppc_35, (0) :: double precision))) +
                        COALESCE(ref_d18_ppc.ppc_36, (0) :: double precision)) +
                       COALESCE(ref_d18_ppc.ppc_37, (0) :: double precision)) +
                      COALESCE(ref_d18_ppc.ppc_38, (0) :: double precision)) +
                     COALESCE(ref_d18_ppc.ppc_39, (0) :: double precision)) +
                    COALESCE(ref_d18_ppc.ppc_40, (0) :: double precision)) +
                   COALESCE(ref_d18_ppc.ppc_41, (0) :: double precision)) +
                  COALESCE(ref_d18_ppc.ppc_42, (0) :: double precision)) +
                 COALESCE(ref_d18_ppc.ppc_43, (0) :: double precision)) +
                COALESCE(ref_d18_ppc.ppc_44, (0) :: double precision)) +
               COALESCE((+ref_d18_ppc.ppc_45), (0) :: double precision)) +
              COALESCE(ref_d18_ppc.ppc_46, (0) :: double precision)) +
             COALESCE(ref_d18_ppc.ppc_47, (0) :: double precision)) +
            COALESCE(ref_d18_ppc.ppc_48, (0) :: double precision)) +
           COALESCE(ref_d18_ppc.ppc_49, (0) :: double precision)) +
          COALESCE(ref_d18_ppc.ppc_50, (0) :: double precision)) AS ppc_sum
from ref_d18_ppc
where ss_conf_id ='0393'
    and pcl_id = 1
    and gsp_group_id ='_H'
and st_date='20190119'


         select    ((((((((((((((((((((((((((((((((
                                          COALESCE(ref_d18_ppc.ppc_18, (0) :: double precision) +
                                         COALESCE(ref_d18_ppc.ppc_19, (0) :: double precision)) +
                                        COALESCE(ref_d18_ppc.ppc_20, (0) :: double precision)) +
                                       COALESCE(ref_d18_ppc.ppc_21, (0) :: double precision)) +
                                      COALESCE(ref_d18_ppc.ppc_22, (0) :: double precision)) +
                                     COALESCE(ref_d18_ppc.ppc_23, (0) :: double precision)) +
                                    COALESCE(ref_d18_ppc.ppc_24, (0) :: double precision)) +
                                   COALESCE(ref_d18_ppc.ppc_25, (0) :: double precision)) +
                                  COALESCE(ref_d18_ppc.ppc_26, (0) :: double precision)) +
                                 COALESCE(ref_d18_ppc.ppc_27, (0) :: double precision)) +
                                COALESCE(ref_d18_ppc.ppc_28, (0) :: double precision)) +
                               COALESCE(ref_d18_ppc.ppc_29, (0) :: double precision)) +
                              COALESCE(ref_d18_ppc.ppc_30, (0) :: double precision)) +
                             COALESCE(ref_d18_ppc.ppc_31, (0) :: double precision)) +
                            COALESCE(ref_d18_ppc.ppc_32, (0) :: double precision)) +
                           COALESCE(ref_d18_ppc.ppc_33, (0) :: double precision)) +
                          COALESCE(ref_d18_ppc.ppc_34, (0) :: double precision)) +
                         (+COALESCE(ref_d18_ppc.ppc_35, (0) :: double precision))) +
                        COALESCE(ref_d18_ppc.ppc_36, (0) :: double precision)) +
                       COALESCE(ref_d18_ppc.ppc_37, (0) :: double precision)) +
                      COALESCE(ref_d18_ppc.ppc_38, (0) :: double precision)) +
                     COALESCE(ref_d18_ppc.ppc_39, (0) :: double precision)) +
                    COALESCE(ref_d18_ppc.ppc_40, (0) :: double precision)) +
                   COALESCE(ref_d18_ppc.ppc_41, (0) :: double precision)) +
                  COALESCE(ref_d18_ppc.ppc_42, (0) :: double precision)) +
                 COALESCE(ref_d18_ppc.ppc_43, (0) :: double precision)) +
                COALESCE(ref_d18_ppc.ppc_44, (0) :: double precision)) +
               COALESCE((+ref_d18_ppc.ppc_45), (0) :: double precision)) +
              COALESCE(ref_d18_ppc.ppc_46, (0) :: double precision)) +
             COALESCE(ref_d18_ppc.ppc_47, (0) :: double precision)) +
            COALESCE(ref_d18_ppc.ppc_48, (0) :: double precision)) +
           COALESCE(ref_d18_ppc.ppc_49, (0) :: double precision)) +
          COALESCE(ref_d18_ppc.ppc_50, (0) :: double precision)) AS ppc_sum
from ref_d18_ppc
where ss_conf_id ='0393'
    and pcl_id = 1
    and gsp_group_id ='_H'
and st_date='20190219'


select sum(ppc_sum)
              from ref_d18_igloo_ppc_bak_26042019
             where ss_conf_id ='0393'
    and pcl_id = 1
    and gsp_group_id ='_H'
                and st_date >= '20190120'
                and st_date <= '20190218'
              group by gsp_group_id, ss_conf_id

 select sum((((((((((((((((((((((((((((((((((((((((((((((((((COALESCE(ref_d18_ppc.ppc_1, (0) :: double precision) +
                                                          COALESCE(ref_d18_ppc.ppc_2, (0) :: double precision)) +
                                                         COALESCE(ref_d18_ppc.ppc_3, (0) :: double precision)) +
                                                        COALESCE(ref_d18_ppc.ppc_4, (0) :: double precision)) +
                                                       COALESCE(ref_d18_ppc.ppc_5, (0) :: double precision)) +
                                                      COALESCE(ref_d18_ppc.ppc_6, (0) :: double precision)) +
                                                     COALESCE(ref_d18_ppc.ppc_7, (0) :: double precision)) +
                                                    COALESCE(ref_d18_ppc.ppc_8, (0) :: double precision)) +
                                                   COALESCE(ref_d18_ppc.ppc_9, (0) :: double precision)) +
                                                  COALESCE(ref_d18_ppc.ppc_10, (0) :: double precision)) +
                                                 COALESCE(ref_d18_ppc.ppc_11, (0) :: double precision)) +
                                                COALESCE(ref_d18_ppc.ppc_12, (0) :: double precision)) +
                                               COALESCE(ref_d18_ppc.ppc_13, (0) :: double precision)) +
                                              COALESCE(ref_d18_ppc.ppc_14, (0) :: double precision)) +
                                             COALESCE(ref_d18_ppc.ppc_15, (0) :: double precision)) +
                                            COALESCE(ref_d18_ppc.ppc_16, (0) :: double precision)) +
                                           COALESCE(ref_d18_ppc.ppc_17, (0) :: double precision)) +
                                          COALESCE(ref_d18_ppc.ppc_18, (0) :: double precision)) +
                                         COALESCE(ref_d18_ppc.ppc_19, (0) :: double precision)) +
                                        COALESCE(ref_d18_ppc.ppc_20, (0) :: double precision)) +
                                       COALESCE(ref_d18_ppc.ppc_21, (0) :: double precision)) +
                                      COALESCE(ref_d18_ppc.ppc_22, (0) :: double precision)) +
                                     COALESCE(ref_d18_ppc.ppc_23, (0) :: double precision)) +
                                    COALESCE(ref_d18_ppc.ppc_24, (0) :: double precision)) +
                                   COALESCE(ref_d18_ppc.ppc_25, (0) :: double precision)) +
                                  COALESCE(ref_d18_ppc.ppc_26, (0) :: double precision)) +
                                 COALESCE(ref_d18_ppc.ppc_27, (0) :: double precision)) +
                                COALESCE(ref_d18_ppc.ppc_28, (0) :: double precision)) +
                               COALESCE(ref_d18_ppc.ppc_29, (0) :: double precision)) +
                              COALESCE(ref_d18_ppc.ppc_30, (0) :: double precision)) +
                             COALESCE(ref_d18_ppc.ppc_31, (0) :: double precision)) +
                            COALESCE(ref_d18_ppc.ppc_32, (0) :: double precision)) +
                           COALESCE(ref_d18_ppc.ppc_33, (0) :: double precision)) +
                          COALESCE(ref_d18_ppc.ppc_34, (0) :: double precision)) +
                         (+COALESCE(ref_d18_ppc.ppc_35, (0) :: double precision))) +
                        COALESCE(ref_d18_ppc.ppc_36, (0) :: double precision)) +
                       COALESCE(ref_d18_ppc.ppc_37, (0) :: double precision)) +
                      COALESCE(ref_d18_ppc.ppc_38, (0) :: double precision)) +
                     COALESCE(ref_d18_ppc.ppc_39, (0) :: double precision)) +
                    COALESCE(ref_d18_ppc.ppc_40, (0) :: double precision)) +
                   COALESCE(ref_d18_ppc.ppc_41, (0) :: double precision)) +
                  COALESCE(ref_d18_ppc.ppc_42, (0) :: double precision)) +
                 COALESCE(ref_d18_ppc.ppc_43, (0) :: double precision)) +
                COALESCE(ref_d18_ppc.ppc_44, (0) :: double precision)) +
               COALESCE((+ref_d18_ppc.ppc_45), (0) :: double precision)) +
              COALESCE(ref_d18_ppc.ppc_46, (0) :: double precision)) +
             COALESCE(ref_d18_ppc.ppc_47, (0) :: double precision)) +
            COALESCE(ref_d18_ppc.ppc_48, (0) :: double precision)) +
           COALESCE(ref_d18_ppc.ppc_49, (0) :: double precision)) +
          COALESCE(ref_d18_ppc.ppc_50, (0) :: double precision)))
from ref_d18_ppc
where ss_conf_id ='0393'
 and st_date >= '20190120'
                and st_date <= '20190219'
    and pcl_id = 1
    and gsp_group_id ='_H'




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
--       st.read_min_datetime_elec_created,
--       st.read_max_datetime_elec_created,
       datediff(hours, st.read_min_datetime_elec, st.read_max_datetime_elec)  as read_days_diff_elec,
--        datediff(hours , st.read_min_datetime_elec_created, st.read_max_datetime_elec_created)     as read_days_diff_elec_created,

       st.read_consumption_elec,
       ppc,
--        case
--          when ppc != 0 then ((1 / ppc) * st.read_consumption_elec)
--          else 0 end                                                                     as igloo_eac,
       st.total_reads                                                         as total_reads,
       st.rv_register_id                                                      as rv_register_id,
       register_eac_elec                                                      as industry_eac_register,
       st.previous_eac                                                        as previous_ind_eac_estimates,
       st.latest_eac                                                          as latest_ind_eac_estimates,
       case
         when (ppc != 0 or ppc is not null) and st.previous_eac != 0
                 then ((2 * ppc) * (st.read_consumption_elec / ppc) + (1 - (2 * ppc)) * st.previous_eac)
         else 0 end                                                           as igloo_eac_v1,
       case
         when (ppc != 0 or ppc is not null) and st.previous_eac != 0
                 then st.latest_eac - ((2 * ppc) * (st.read_consumption_elec / ppc) + (1 - (2 * ppc)) * st.previous_eac)
         else 0 end                                                              ind_minus_igloo_eac,
       case
         when st.latest_eac != 0 and (ppc != 0 or ppc is not null) and st.previous_eac != 0
                 then (st.latest_eac /
                       ((2 * ppc) * (st.read_consumption_elec / ppc) + (1 - (2 * ppc)) * st.previous_eac))
         else 0 end                                                              ind_igloo_perc,
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
--              min(read_valid.meterreadingcreateddate)                                 as read_min_datetime_elec_created,
--              max(read_valid.meterreadingcreateddate)                                 as read_max_datetime_elec_created,

             min(read_valid.corrected_reading)                                    as read_min_readings_elec,
             max(read_valid.corrected_reading)                                    as read_max_readings_elec,
             max(read_valid.corrected_reading) - min(read_valid.corrected_reading)as read_consumption_elec,
--              (select sum(ppc_sum)
--               from ref_d18_igloo_ppc
--               where gsp_group_id = rma_gsp.attributes_attributevalue
--                 and ss_conf_id = rma_ssc.attributes_attributevalue
--                 and cast(time_pattern_regime as bigint) = reg_elec.registers_tpr
--                 and pcl_id = 1
--                 and st_date > min(read_valid.meterreadingdatetime)
--                 and st_date <= max(read_valid.meterreadingdatetime)
--               group by gsp_group_id, ss_conf_id)                                     ppc,
0.078958216 as ppc,

             read_valid.register_id                                               as rv_register_id,
             read_valid.total_reads                                               as total_reads,
             2                                                                    as smooth_factor,
--              read_valid.previous_eac                                              as previous_eac,
2573.6 as previous_eac,
             read_valid.latest_eac                                                as latest_eac
      from ref_meterpoints mp_elec
             inner join ref_meterpoints_attributes rma_gsp on mp_elec.meter_point_id = rma_gsp.meter_point_id and
                                                              rma_gsp.attributes_attributename = 'GSP'
             inner join ref_meterpoints_attributes rma_ssc on mp_elec.meter_point_id = rma_ssc.meter_point_id and
                                                              rma_ssc.attributes_attributename = 'SSC'
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
                                and y.total_reads >= 4) read_valid on read_valid.register_id = reg_elec.register_id

      where
        mp_elec.account_id = 4601 and
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
               read_valid.total_reads,
               read_valid.previous_eac,
               read_valid.latest_eac,
               read_valid.register_id) st


SELECT * FROM ref_registers
where register_id =6804

select * from ref_estimates_elec_internal
where account_id = 4601



select * from ref_readings_internal_valid_bak_26042019
where account_id =4601

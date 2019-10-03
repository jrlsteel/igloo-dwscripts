with read_types as
         (select account_id,
                 meter_id,
                 min(ssd_date) as ssd_date,
                 case min(ssd_cat)
                     when 0 then 'No'
                     when 1 then 'Invalid'
                     when 2 then 'Valid'
                     end       as ssd_cat,
                 min(sed_date) as sed_date,
                 case min(sed_cat)
                     when 0 then 'No'
                     when 1 then 'Invalid_Negative'
                     when 2 then 'Invalid_Positive'
                     when 3 then 'Valid_Negative'
                     when 4 then 'Valid_Positive'
                     end       as sed_cat,
                 min(i_date)   as i_date,
                 case min(i_cat)
                     when 0 then 'No'
                     when 1 then 'Invalid'
                     when 2 then 'Valid'
                     end       as i_cat,
                 min(f_date)   as f_date,
                 case min(f_cat)
                     when 0 then 'No'
                     when 1 then 'Invalid_Negative'
                     when 2 then 'Invalid_Positive'
                     when 3 then 'Valid_Negative'
                     when 4 then 'Valid_Positive'
                     end       as f_cat
          from (select reg.account_id,
                       reg.meter_id,
                       reg.register_id,
                       max(rri_open.meterreadingdatetime)    as ssd_date,
                       max(case
                               when rri_open.meterreadingstatusuid is null then 0
                               when rri_open.meterreadingstatusuid != 'VALID' then 1
                               else 2
                           end)                              as ssd_cat,
                       max(rri_initial.meterreadingdatetime) as i_date,
                       max(case
                               when rri_initial.meterreadingstatusuid is null then 0
                               when rri_initial.meterreadingstatusuid != 'VALID' then 1
                               else 2
                           end)                              as i_cat,
                       max(rri_close.meterreadingdatetime)   as sed_date,
                       max(case
                               when rri_close.hasregisteradvance is null or
                                    rri_close.meterreadingstatusuid is null then 0
                               when rri_close.meterreadingstatusuid != 'VALID' and
                                    not rri_close.hasregisteradvance then 1
                               when rri_close.meterreadingstatusuid != 'VALID' and
                                    rri_close.hasregisteradvance then 2
                               when rri_close.meterreadingstatusuid = 'VALID' and
                                    not rri_close.hasregisteradvance then 3
                               else 4
                           end)                              as sed_cat,
                       max(rri_final.meterreadingdatetime)   as f_date,
                       max(case
                               when rri_final.hasregisteradvance is null or
                                    rri_final.meterreadingstatusuid is null then 0
                               when rri_final.meterreadingstatusuid != 'VALID' and
                                    not rri_final.hasregisteradvance then 1
                               when rri_final.meterreadingstatusuid != 'VALID' and
                                    rri_final.hasregisteradvance then 2
                               when rri_final.meterreadingstatusuid = 'VALID' and
                                    not rri_final.hasregisteradvance then 3
                               else 4
                           end)                              as f_cat
                from ref_meters met
                         inner join ref_meterpoints_raw rmp
                                    on met.account_id = rmp.account_id and
                                       met.meter_point_id = rmp.meter_point_id
                         inner join ref_registers reg
                                    on reg.account_id = met.account_id and reg.meter_id = met.meter_id
                         left join ref_readings_internal rri_open
                                   on rri_open.account_id = reg.account_id and
                                      rri_open.register_id = reg.register_id and
                                      rri_open.meterreadingdatetime =
                                      greatest(rmp.supplystartdate, rmp.associationstartdate) and
                                      rri_open.meterreadingsourceuid in ('DC', 'DCOPENING')
                         left join ref_readings_internal rri_close
                                   on rri_close.account_id = reg.account_id and
                                      rri_close.register_id = reg.register_id and
                                      datediff(days,
                                               least(rmp.supplyenddate, rmp.associationenddate),
                                               rri_close.meterreadingdatetime) = 1 and
                                      rri_close.meterreadingsourceuid = 'DC'
                         left join ref_readings_internal rri_initial
                                   on rri_initial.account_id = reg.account_id and
                                      rri_initial.register_id = reg.register_id and
                                      rri_initial.meterreadingdatetime = met.installeddate and
                                      rri_initial.meterreadingsourceuid = 'DC'
                         left join ref_readings_internal rri_final
                                   on rri_final.account_id = reg.account_id and
                                      rri_final.register_id = reg.register_id and
                                      rri_final.meterreadingdatetime = met.removeddate and
                                      rri_final.meterreadingsourceuid = 'DC'
                group by reg.account_id, reg.meter_id, reg.register_id) reg_level
          group by reg_level.account_id, reg_level.meter_id
          order by account_id, meter_id)

select mp.meterpointnumber                                                    as MPR,
       met.meterserialnumber                                                  as MSN,
       met.account_id                                                         as Account_ID,
       mp.meterpointtype                                                      as fuel_type,
       mp.supplystartdate                                                     as meterpoint_SSD,
       mp.supplyenddate                                                       as meterpoint_SED,
       udf_meterpoint_status(mp.supplystartdate, mp.supplyenddate)            as meterpoint_status,
       greatest(mp.supplystartdate, mp.associationstartdate)                  as acc_mp_SSD,
       least(mp.supplyenddate, mp.associationenddate)                         as acc_mp_SED,
       udf_meterpoint_status(acc_mp_SSD, acc_mp_SED)                          as acc_mp_status,
       rma_status.metersattributes_attributevalue                             as meter_status, -- only present in around half of cases
       met.installeddate                                                      as meter_install_date,
       met.removeddate                                                        as meter_removed_date,
       rma_type.metersattributes_attributevalue                               as meter_type,
       rma_location.metersattributes_attributevalue                           as meter_location,
       rma_mech.metersattributes_attributevalue                               as meter_mechanism,
       num_reg.reg_count                                                      as num_registers,
       rma_digits.attributes_attributevalue                                   as num_dials,
       rma_ssc.attributes_attributevalue                                      as SSC,
       rma_mopmam.attributes_attributevalue                                   as MOP_MAM,
       rma_mopmam.attributes_effectivefromdate                                as MOP_MAM_effective_date,
--        null                                                                  as MAM,
--        null                                                                  as MAM_effective_date,
       old_mopmams.old_mopmams                                                as old_MOP_MAM,
--        null                                                                  as old_MAM,
       rma_osmopmam.attributes_attributevalue                                 as old_supplier_MOP_MAM,
--        null                                                                  as old_supplier_MAM,
       case
           when met_repl.meter_id is not null then 'Yes'
           when met.removeddate is not null and met.removeddate < getdate() then 'Removed'
           else 'No'
           end                                                                as MEX_occurred,
       met_repl.installeddate                                                 as MEX_date,
       case
           when met.removeddate is null or met.removeddate > getdate() then 'N/A'
           else read_info.f_cat
           end                                                                as F_read,
       case when F_read = 'N/A' then null else read_info.f_date end           as F_read_date,
       case
           when met.installeddate < acc_mp_SSD then 'N/A'
           else read_info.i_cat
           end                                                                as I_read,
       case when I_read = 'N/A' then null else read_info.i_date end           as I_read_date,
       case
           when acc_mp_SSD > getdate() then 'N/A'
           else read_info.ssd_cat
           end                                                                as SSD_DC_read_in,
       case when SSD_DC_read_in = 'N/A' then null else read_info.ssd_date end as SSD_DC_read_date,
       case
           when acc_mp_SED is null or acc_mp_SED > getdate() then 'N/A'
           else read_info.sed_cat
           end                                                                as SED_DC_Read_in,
       case when SED_DC_Read_in = 'N/A' then null else read_info.sed_date end as SED_DC_read_date,
       case
           when estimates.num_reg is null then 'No'
           when estimates.num_reg < num_registers then 'Partial'
           else 'Yes'
           end                                                                as EAC_AQ_in,
       estimates.effective_from                                               as EAC_AQ_effective_date
--        null                                                        as AQ_in,
--        null                                                        as AQ_effective_date,


from ref_meters met
         left join read_types read_info on met.account_id = read_info.account_id and
                                           met.meter_id = read_info.meter_id
         left join ref_meterpoints mp on met.meter_point_id = mp.meter_point_id and met.account_id = mp.account_id
         left join ref_meters_attributes rma_status on rma_status.metersattributes_attributename = 'Meter_Status' and
                                                       rma_status.meter_id = met.meter_id and
                                                       rma_status.account_id = met.account_id
         left join ref_meters_attributes rma_type on rma_type.metersattributes_attributename = 'MeterType' and
                                                     rma_type.meter_id = met.meter_id and
                                                     rma_type.account_id = met.account_id
         left join ref_meters_attributes rma_location
                   on rma_location.metersattributes_attributename = 'Meter_Location' and
                      rma_location.meter_id = met.meter_id and
                      rma_location.account_id = met.account_id
         left join ref_meters_attributes rma_mech on rma_mech.metersattributes_attributename = 'Gas_Meter_Mechanism' and
                                                     rma_mech.meter_id = met.meter_id and
                                                     rma_mech.account_id = met.account_id
         left join (select account_id, meter_id, count(distinct register_id) as reg_count
                    from ref_registers
                    group by account_id, meter_id) num_reg on num_reg.meter_id = met.meter_id and
                                                              num_reg.account_id = met.account_id
         left join ref_meterpoints_attributes rma_digits
                   on rma_digits.attributes_attributename in ('No_Of_Digits', 'Gas_No_Of_Digits') and
                      rma_digits.meter_point_id = met.meter_point_id and
                      rma_digits.account_id = met.account_id
         left join ref_meterpoints_attributes rma_ssc on rma_ssc.attributes_attributename = 'SSC' and
                                                         rma_ssc.meter_point_id = met.meter_point_id and
                                                         rma_ssc.account_id = met.account_id
         left join ref_meterpoints_attributes rma_mopmam on rma_mopmam.attributes_attributename in ('MOP', 'MAM') and
                                                            rma_mopmam.meter_point_id = met.meter_point_id and
                                                            rma_mopmam.account_id = met.account_id and
                                                            (rma_mopmam.attributes_effectivetodate is null or
                                                             rma_mopmam.attributes_effectivetodate > getdate())
         left join (select account_id, meter_point_id, listagg(distinct attributes_attributevalue) as old_mopmams
                    from ref_meterpoints_attributes
                    where attributes_attributename in ('MOP', 'MAM')
                      and attributes_effectivetodate is not null
                      and attributes_effectivetodate < getdate()
                    group by account_id, meter_point_id) old_mopmams on old_mopmams.account_id = met.account_id and
                                                                        old_mopmams.meter_point_id = met.meter_point_id
         left join ref_meterpoints_attributes rma_osmopmam
                   on rma_osmopmam.attributes_attributename in ('OLD_SUPPLIER_MOP', 'OLD_SUPPLIER_MAM') and
                      rma_osmopmam.account_id = met.account_id and rma_osmopmam.meter_point_id = met.meter_point_id
         left join ref_meters met_repl --todo - join ref meters to readings internal here (inner select) to get I_read
                   on met_repl.account_id = met.account_id and met_repl.meter_point_id = met.meter_point_id and
                      datediff(days, met.removeddate, met_repl.installeddate) between 0 and 5
         left join (select account_id, mpan, serial_number, count(*) as num_reg, min(effective_from) as effective_from
                    from (select account_id, mpan, serial_number, register_id, max(effective_from) as effective_from
                          from (select *
                                from ref_estimates_elec_internal
                                union
                                select *
                                from ref_estimates_gas_internal) ests
                          group by account_id, mpan, serial_number, register_id) most_recent_estimate
                    group by account_id, mpan, serial_number) estimates
                   on met.account_id = estimates.account_id and met.meterserialnumber = estimates.serial_number

order by mpr, msn, Account_ID
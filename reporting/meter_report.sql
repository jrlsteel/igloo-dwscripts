drop table temp_mr_dev;
create table temp_mr_dev as

with readings as (select rmp.account_id,
                         rmp.meter_point_id,
                         met.meter_id,
                         read_info.reading_date,
                         read_info.dc_read,
                         count(reg.register_id) > count(read_info.register_id) as some_reg_missing_reads,
                         sum((source in ('DC', 'CUSTOMER', 'SMART'))::int) > 0 as cdcs_read, -- Customer, DC or Smart reading
                         listagg(distinct source, ',')                         as source,
                         case min(weight)
                             when 0 then 'Invalid_Negative'
                             when 1 then 'Invalid_Positive'
                             when 2 then 'Valid_Negative'
                             when 3 then 'Valid_Positive'
                             end                                               as status,
                         min(weight) in (2, 3) and cdcs_read                   as valid_actual,
                         min(weight) in (0, 1) and cdcs_read                   as invalid_actual
                  from ref_meterpoints rmp
                           left join ref_meters met
                                     on met.account_id = rmp.account_id and met.meter_point_id = rmp.meter_point_id
                           left join ref_registers reg
                                     on reg.account_id = met.account_id and reg.meter_id = met.meter_id and
                                        reg.registers_tprperioddescription is not null
                           left join (select account_id,
                                             meter_point_id,
                                             meter_id,
                                             register_id,
                                             reading_date,
                                             source,
                                             dc_read,
                                             weight
                                      from (select account_id,
                                                   meter_point_id,
                                                   meter_id,
                                                   register_id,
                                                   trunc(meterreadingdatetime)                         as reading_date,
                                                   meterreadingsourceuid                               as source,

                                                   left(meterreadingsourceuid, 2) = 'DC'               as dc_read,
                                                   meterreadingstatusuid = 'VALID'                     as reading_valid,
                                                   hasregisteradvance                                  as reading_advance,
                                                   (reading_advance::int) + (2 * (reading_valid::int)) as weight,
                                                   row_number() over (partition by
                                                       account_id,
                                                       register_id,
                                                       reading_date,
                                                       dc_read
                                                       order by weight desc)                           as rn
                                            from ref_readings_internal
                                            where meterreadingtypeuid = 'ACTUAL') readings
                                      where rn = 1
                                      order by account_id, reading_date) read_info
                                     on read_info.account_id = reg.account_id and
                                        read_info.register_id = reg.register_id
                  group by rmp.account_id, rmp.meter_point_id, met.meter_id, read_info.reading_date, read_info.dc_read)

select mp.meterpointnumber                                                      as MPR,
       met.meterserialnumber                                                    as MSN,
       met.account_id                                                           as Account_ID,
       mp.meterpointtype                                                        as fuel_type,
       mp.supplystartdate                                                       as meterpoint_SSD,
       mp.supplyenddate                                                         as meterpoint_SED,
       udf_meterpoint_status(mp.supplystartdate, mp.supplyenddate)              as meterpoint_status,
       greatest(mp.supplystartdate, mp.associationstartdate)                    as acc_mp_SSD,
       least(mp.supplyenddate, mp.associationenddate)                           as acc_mp_SED,
       udf_meterpoint_status(acc_mp_SSD, acc_mp_SED)                            as acc_mp_status,
       rma_status.metersattributes_attributevalue                               as meter_status, -- only present in around half of cases
       met.installeddate                                                        as meter_install_date,
       met.removeddate                                                          as meter_removed_date,
       rma_type.metersattributes_attributevalue                                 as meter_type,
       rma_location.metersattributes_attributevalue                             as meter_location,
       rma_mech.metersattributes_attributevalue                                 as meter_mechanism,
       num_reg.reg_count                                                        as num_registers,
       rma_digits.attributes_attributevalue                                     as num_dials,
       rma_ssc.attributes_attributevalue                                        as SSC,
       rma_mopmam.attributes_attributevalue                                     as MOP_MAM,
       rma_mopmam.attributes_effectivefromdate                                  as MOP_MAM_effective_date,
       old_mopmams.old_mopmams                                                  as old_MOP_MAM,
       rma_osmopmam.attributes_attributevalue                                   as old_supplier_MOP_MAM,
       case
           when met_repl.meter_id is not null then 'Yes'
           when met.removeddate is not null and met.removeddate < getdate() then 'Removed'
           else 'No'
           end                                                                  as MEX_occurred,
       met_repl.installeddate                                                   as MEX_date,

       -- F_Readings
       case
           when met.removeddate is null or met.removeddate > getdate() then null
           else met.removeddate
           end                                                                  as F_read_date,
       case
           when F_read_date is null then 'N/A'
           when f_read_dc.status is null then 'No'
           when f_read_dc.some_reg_missing_reads then 'Partial'
           else f_read_dc.status end                                            as F_read_dc,
       case
           when F_read_date is null then 'N/A'
           else f_read_other.source end                                         as F_read_other_source,
       case
           when F_read_date is null then 'N/A'
           when f_read_other.status is null then 'No'
           when f_read_other.some_reg_missing_reads then 'Partial'
           else f_read_other.status end                                         as F_read_other_status,

       -- I_Readings
       case
           when met.installeddate < acc_mp_SSD then null
           else met.installeddate
           end                                                                  as I_read_date,
       case
           when I_read_date is null then 'N/A'
           when i_read_dc.status is null then 'No'
           when i_read_dc.some_reg_missing_reads then 'Partial'
           else left(i_read_dc.status, len(i_read_dc.status) - 9) end           as I_read_dc,
       case
           when I_read_date is null then 'N/A'
           else i_read_other.source end                                         as I_read_other_source,
       case
           when I_read_date is null then 'N/A'
           when i_read_other.status is null then 'No'
           when i_read_other.some_reg_missing_reads then 'Partial'
           else left(i_read_other.status, len(i_read_other.status) - 9) end     as I_read_other_status,

       -- SED_Readings
       case
           when acc_mp_SED is null or acc_mp_SED > getdate() or nvl(met.removeddate, acc_mp_SED + 1) < acc_mp_SED
               then null
           else acc_mp_SED
           end                                                                  as SED_read_date,
       case
           when SED_read_date is null then 'N/A'
           when sED_read_dc.status is null then 'No'
           when sED_read_dc.some_reg_missing_reads then 'Partial'
           else sED_read_dc.status end                                          as SED_read_dc,
       case
           when SED_read_date is null then 'N/A'
           else sED_read_other.source end                                       as SED_read_other_source,
       case
           when SED_read_date is null then 'N/A'
           when sED_read_other.status is null then 'No'
           when sED_read_other.some_reg_missing_reads then 'Partial'
           else sED_read_other.status end                                       as SED_read_other_status,

       -- SSD_Readings
       case
           when acc_mp_SSD > getdate() or acc_mp_SSD < met.installeddate then null
           else acc_mp_SSD
           end                                                                  as SSD_read_date,
       case
           when SSD_read_date is null then 'N/A'
           when sSD_read_dc.status is null then 'No'
           when sSD_read_dc.some_reg_missing_reads then 'Partial'
           else left(sSD_read_dc.status, len(ssd_read_dc.status) - 9) end       as SSD_read_dc,
       case
           when SSD_read_date is null then 'N/A'
           else sSD_read_other.source end                                       as SSD_read_other_source,
       case
           when SSD_read_date is null then 'N/A'
           when sSD_read_other.status is null then 'No'
           when sSD_read_other.some_reg_missing_reads then 'Partial'
           else left(sSD_read_other.status, len(ssd_read_other.status) - 9) end as SSD_read_other_status,

       case
           when estimates.num_reg is null then 'No'
           when estimates.num_reg < num_registers then 'Partial'
           else 'Yes'
           end                                                                  as EAC_AQ_in,
       estimates.effective_from                                                 as EAC_AQ_effective_date,
       rma_sup_stat.attributes_attributevalue                                   as supply_status,
       rma_imp.attributes_attributevalue                                        as gas_imperial_indicator,
       rma_gain_sup.attributes_attributevalue                                   as gain_supplier,
       neg_states.neg_state_occurred                                            as neg_state_occurred,
       read_summaries.num_valid_actual_reads,
       read_summaries.num_inv_actual_reads,
       read_summaries.first_valid_actual,
       read_summaries.first_invalid_actual,
       read_summaries.latest_valid_actual,
       read_summaries.latest_invalid_actual,
       lia_type.status                                                          as latest_invalid_actual_type,
       getdate()                                                                as etlchange
from ref_meterpoints mp
         left join ref_meters met on met.meter_point_id = mp.meter_point_id and met.account_id = mp.account_id
         left join readings i_read_dc on met.account_id = i_read_dc.account_id and
                                         met.meter_id = i_read_dc.meter_id and
                                         met.installeddate = i_read_dc.reading_date and
                                         i_read_dc.dc_read = true
         left join readings i_read_other on met.account_id = i_read_other.account_id and
                                            met.meter_id = i_read_other.meter_id and
                                            met.installeddate = i_read_other.reading_date and
                                            i_read_other.dc_read = false
         left join readings f_read_dc on met.account_id = f_read_dc.account_id and
                                         met.meter_id = f_read_dc.meter_id and
                                         met.removeddate = f_read_dc.reading_date and
                                         f_read_dc.dc_read = true
         left join readings f_read_other on met.account_id = f_read_other.account_id and
                                            met.meter_id = f_read_other.meter_id and
                                            met.removeddate = f_read_other.reading_date and
                                            f_read_other.dc_read = false
         left join readings ssd_read_dc on met.account_id = ssd_read_dc.account_id and
                                           met.meter_id = ssd_read_dc.meter_id and
                                           greatest(mp.supplystartdate, mp.associationstartdate) =
                                           ssd_read_dc.reading_date and
                                           ssd_read_dc.dc_read = true
         left join readings ssd_read_other on met.account_id = ssd_read_other.account_id and
                                              met.meter_id = ssd_read_other.meter_id and
                                              greatest(mp.supplystartdate, mp.associationstartdate) =
                                              ssd_read_other.reading_date and
                                              ssd_read_other.dc_read = false
         left join readings sed_read_dc on met.account_id = sed_read_dc.account_id and
                                           met.meter_id = sed_read_dc.meter_id and
                                           least(mp.supplyenddate, mp.associationenddate) = sed_read_dc.reading_date and
                                           sed_read_dc.dc_read = true
         left join readings sed_read_other on met.account_id = sed_read_other.account_id and
                                              met.meter_id = sed_read_other.meter_id and
                                              least(mp.supplyenddate, mp.associationenddate) =
                                              sed_read_other.reading_date and
                                              sed_read_other.dc_read = false
         left join ref_meters_attributes rma_status on rma_status.metersattributes_attributename = 'Meter_Status' and
                                                       rma_status.meter_id = met.meter_id and
                                                       rma_status.account_id = met.account_id
         left join ref_meters_attributes rma_type on rma_type.metersattributes_attributename = 'MeterType' and
                                                     rma_type.meter_id = met.meter_id and
                                                     rma_type.account_id = met.account_id
         left join ref_meters_attributes rma_location
                   on rma_location.metersattributes_attributename ilike 'Meter_Location' and
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
         left join ref_meters met_repl
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
         left join ref_meterpoints_attributes rma_sup_stat on rma_sup_stat.account_id = mp.account_id and
                                                              rma_sup_stat.meter_point_id = mp.meter_point_id and
                                                              rma_sup_stat.attributes_attributename ilike 'Supply_Status'
         left join ref_meterpoints_attributes rma_imp on rma_imp.account_id = mp.account_id and
                                                         rma_imp.meter_point_id = mp.meter_point_id and
                                                         rma_imp.attributes_attributename ilike 'gas_imperial_meter_indicator'
         left join ref_meterpoints_attributes rma_gain_sup on rma_gain_sup.account_id = mp.account_id and
                                                              rma_gain_sup.meter_point_id = mp.meter_point_id and
                                                              rma_gain_sup.attributes_attributename ilike 'gain_supplier'
         left join (select account_id,
                           meter_point_id,
                           sum((attributes_attributevalue in ('CANCELLED_IN_COOLING_OFF',
                                                              'OBJECTION_UPHELD',
                                                              'REGISTRATION_CANCELLED',
                                                              'REGISTRATION_REJECTED',
                                                              'WITHDRAWAL_SENT'))::int) > 0 as neg_state_occurred
                    from ref_meterpoints_attributes_audit
                    where attributes_attributename ilike 'supply_status'
                    group by account_id, meter_point_id) neg_states on mp.account_id = neg_states.account_id and
                                                                       mp.meter_point_id = neg_states.meter_point_id
         left join (select account_id,
                           meter_point_id,
                           meter_id,
                           sum(valid_actual::int)                                        as num_valid_actual_reads,
                           sum(invalid_actual::int)                                      as num_inv_actual_reads,
                           min(case when valid_actual then reading_date else null end)   as first_valid_actual,
                           min(case when invalid_actual then reading_date else null end) as first_invalid_actual,
                           max(case when valid_actual then reading_date else null end)   as latest_valid_actual,
                           max(case when invalid_actual then reading_date else null end) as latest_invalid_actual
                    from readings
                    group by account_id, meter_point_id, meter_id) read_summaries
                   on mp.account_id = read_summaries.account_id and
                      mp.meter_point_id = read_summaries.meter_point_id and
                      met.meter_id = read_summaries.meter_id
         left join readings lia_type
                   on lia_type.invalid_actual and lia_type.reading_date = read_summaries.latest_invalid_actual and
                      lia_type.account_id = mp.account_id and lia_type.meter_point_id = mp.meter_point_id and
                      lia_type.meter_id = met.meter_id
order by mpr, msn, Account_ID
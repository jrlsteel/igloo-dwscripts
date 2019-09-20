-- Testing from https://iglooenergy.atlassian.net/browse/DMRE-693
-- Check no duplicates are present in the cons acc elec/gas tables
select count(*)                   as num_records,
       count(distinct account_id) as num_ids,
       'gas'                      as type
from ref_consumption_accuracy_gas
union
select count(*)                   as num_records,
       count(distinct account_id) as num_ids,
       'elec'                     as type
from ref_consumption_accuracy_elec;

-- Check audit tables have picked up on changes
select count(*) as cnt, etlchange, 'gas' as type
from ref_consumption_accuracy_gas_audit
group by etlchange
union
select count(*) as cnt, etlchange, 'elec' as type
from ref_consumption_accuracy_elec_audit
where type = 'elec'
group by etlchange
order by etlchange desc;

-- Check each update was triggered by new calculation input information coming in to the system
select *
from (select account_id,
             reading_datetime,
             pa_cons_gas as pa_cons,
             igl_ind_aq  as igl_ind,
             ind_aq      as ind,
             quotes_aq   as quotes,
             etlchange,
             etlchangetype,
             'gas'       as type
      from ref_consumption_accuracy_gas_audit
      union
      select account_id,
             reading_datetime,
             pa_cons_elec as pa_cons,
             igl_ind_eac  as igl_ind,
             ind_eac      as ind,
             quotes_eac   as quotes,
             etlchange,
             etlchangetype,
             'elec'       as type
      from ref_consumption_accuracy_elec_audit) cons_acc
         left join
     (select account_id,
             max(etlchange)               as most_recent_update_in_period,
             listagg(update_source, ', ') as updates_in_period,
             sum(case
                     when update_source in ('RRIV_ELEC',
                                            'RRIV_GAS',
                                            'EST_ELEC',
                                            'METERPOINTS',
                                            'METERS',
                                            'REGISTERS') then 1
                     else 0 end) > 0      as elec_updated,
             sum(case
                     when update_source in ('RRIV_GAS',
                                            'RRIV_ELEC',
                                            'NRL_START',
                                            'NRL_END',
                                            'EST_GAS',
                                            'METERPOINTS',
                                            'METERS',
                                            'REGISTERS') then 1
                     else 0 end) > 0      as gas_updated
      from (select account_id, etlchange, meterreadingsourceuid as update_source
            from ref_readings_internal_nrl
            union
            select rriv.account_id,
                   rria.etlchange,
                   case when rriv.meterpointtype = 'E' then 'RRIV_ELEC' else 'RRIV_GAS' end as update_source
            from ref_readings_internal_valid rriv
                     left join
                 ref_readings_internal_audit rria
                 on rriv.account_id = rria.account_id and
                    rriv.meter_point_id = rria.meter_point_id and
                    rriv.meter_id = rria.meter_id and
                    rriv.meter_reading_id = rria.meter_reading_id and
                    rriv.register_id = rria.register_id and
                    rriv.register_reading_id = rria.register_reading_id and
                    rriv.billable = rria.billable and
                    rriv.haslivecharge = rria.haslivecharge and
                    rriv.hasregisteradvance = rria.hasregisteradvance and
                    rriv.meterpointnumber = rria.meterpointnumber and
                    rriv.meterpointtype = rria.meterpointtype and
                    rriv.meterreadingcreateddate = rria.meterreadingcreateddate and
                    rriv.meterreadingdatetime = rria.meterreadingdatetime and
                    rriv.meterreadingsourceuid = rria.meterreadingsourceuid and
                    rriv.meterreadingstatusuid = rria.meterreadingstatusuid and
                    rriv.meterreadingtypeuid = rria.meterreadingtypeuid and
                    rriv.meterserialnumber = rria.meterserialnumber and
                    rriv.registerreference = rria.registerreference and
                    rriv.required = rria.required and
                    rriv.readingvalue = rria.readingvalue
            union
            select account_id, etlchange, 'EST_GAS' as update_source
            from ref_estimates_gas_internal_audit
            union
            select account_id, etlchange, 'EST_ELEC' as update_source
            from ref_estimates_elec_internal_audit
            union
            select account_id, etlchange, 'METERPOINTS' as update_source
            from ref_meterpoints_audit
            union
            select account_id, etlchange, 'METERS' as update_source
            from ref_meters_audit
            union
            select account_id, etlchange, 'REGISTERS' as update_source
            from ref_registers_audit
           ) joined_audits
      where etlchange between current_timestamp - 1 and current_timestamp
      group by account_id
      order by account_id) acc_updated
     on acc_updated.account_id = cons_acc.account_id
where cons_acc.etlchange > (current_timestamp - 1)
  and cons_acc.account_id <
      (select min(external_id) from ref_cdb_supply_contracts where created_at > current_timestamp - 30)
  and ((cons_acc.type = 'elec' and (acc_updated.elec_updated is null or not acc_updated.elec_updated))
    or (cons_acc.type = 'gas' and (acc_updated.gas_updated is null or not acc_updated.gas_updated)))
order by cons_acc.account_id

select min(external_id), trunc(created_at) as creation_date
from ref_cdb_supply_contracts
group by creation_date
order by creation_date desc
select *
from ref_cdb_supply_contracts
where external_id = 71620

select *
from ref_consumption_accuracy_gas_audit
where account_id = 8183

select *
from ref_consumption_accuracy_elec

select *
from ref_readings_internal_valid
where account_id = 61986

select max(etlchange)
from ref_consumption_accuracy_elec

select *
from (select distinct account_id
      from ref_meterpoints_raw
      where (end_date > getdate() or end_date is null)
        and meterpointtype = 'E'
        and start_date <= getdate()) elec_live
         left join ref_consumption_accuracy_elec_2019_09_10 cons_acc
                   on elec_live.account_id = cons_acc.account_id
where cons_acc.account_id is null
order by elec_live.account_id

-- Live gas accounts not in cons_acc_gas
select *
from (select distinct account_id
      from ref_meterpoints_raw
      where (end_date > getdate() or end_date is null)
        and meterpointtype = 'G'
        and start_date <= getdate()) gas_live
         left join ref_consumption_accuracy_gas cons_acc
                   on gas_live.account_id = cons_acc.account_id
         left join (select account_id, count(*) as cnt
                    from ref_readings_internal_valid
                    where meterpointtype = 'G'
                    group by account_id) num_valid_reads
                   on gas_live.account_id = num_valid_reads.account_id
         left join (select sc.external_id,
                           mp.account_id is not null  as mp_present,
                           rm.account_id is not null  as rm_present,
                           reg.account_id is not null as reg_present
                    from ref_cdb_supply_contracts sc
                             left join ref_meterpoints mp on sc.external_id = mp.account_id and mp.meterpointtype = 'G'
                             left join ref_meters rm
                                       on rm.meter_point_id = mp.meter_point_id and rm.account_id = mp.account_id and
                                          rm.removeddate is null
                             left join ref_registers reg
                                       on reg.meter_id = rm.meter_id and reg.account_id = rm.account_id) meter_details
                   on meter_details.external_id = gas_live.account_id
where cons_acc.account_id is null
order by gas_live.account_id


-- Live elec accounts not in cons_acc_elec
select *
from (select distinct account_id
      from ref_meterpoints_raw
      where (end_date > getdate() or end_date is null)
        and meterpointtype = 'E'
        and start_date <= getdate()) elec_live
         left join ref_consumption_accuracy_elec cons_acc
                   on elec_live.account_id = cons_acc.account_id
         left join (select account_id, count(*) as cnt
                    from ref_readings_internal_valid
                    where meterpointtype = 'E'
                    group by account_id) num_valid_reads
                   on elec_live.account_id = num_valid_reads.account_id
         left join (select sc.external_id,
                           mp.account_id is not null  as mp_present,
                           rm.account_id is not null  as rm_present,
                           reg.account_id is not null as reg_present
                    from ref_cdb_supply_contracts sc
                             left join ref_meterpoints mp on sc.external_id = mp.account_id and mp.meterpointtype = 'E'
                             left join ref_meters rm
                                       on rm.meter_point_id = mp.meter_point_id and rm.account_id = mp.account_id and
                                          rm.removeddate is null
                             left join ref_registers reg
                                       on reg.meter_id = rm.meter_id and reg.account_id = rm.account_id) meter_details
                   on meter_details.external_id = elec_live.account_id
where cons_acc.account_id is null
order by elec_live.account_id

select acc_aggs.account_id               as account_id,
       latest_readings.max_read_datetime as reading_datetime,
       nvl(acc_aggs.pa_cons_gas, 0)      as pa_cons_gas,
       nvl(acc_aggs.igl_ind_aq, 0)       as igl_ind_aq,
       nvl(acc_aggs.ind_aq, 0)           as ind_aq,
       nvl(case
               when q.gas_usage is null then
                   (q.gas_projected - (3.65 * q.gas_standing)) / (q.gas_unit / 100)
               else q.gas_usage end, 0)  as quotes_aq,
       current_timestamp                 as etlchange
from (select account_id,
             registration_id,
             case
                 when count(nullif(reg_level.pa_cons_gas, 0)) < count(*) then 0
                 else sum(reg_level.pa_cons_gas) end as pa_cons_gas,
             case
                 when count(nullif(reg_level.igl_ind_aq, 0)) < count(*) then 0
                 else sum(reg_level.igl_ind_aq) end  as igl_ind_aq,
             sum(reg_level.ind_aq)                   as ind_aq
      from (select su.external_id          as account_id,
                   su.registration_id,
                   pa_aq.igloo_aq          as pa_cons_gas,
                   calc_aq.igl_ind_aq      as igl_ind_aq,
                   ind_aq.estimation_value as ind_aq
            from ref_cdb_supply_contracts su
                     inner join ref_meterpoints mp
                                on mp.account_id = su.external_id and mp.meterpointtype = 'G'
                     inner join ref_meters mt
                                on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and
                                   mt.removeddate is null
                     inner join ref_registers reg
                                on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
                     left join ref_calculated_aq pa_aq
                               on su.external_id = pa_aq.account_id and
                                  reg.register_id = pa_aq.register_id
                     left join ref_calculated_igl_ind_aq calc_aq
                               on su.external_id = calc_aq.account_id and
                                  reg.register_id = calc_aq.register_id
                     left join (select *,
                                       row_number()
                                       over (partition by account_id,
                                           mprn,
                                           register_id,
                                           serial_number
                                           order by effective_from desc) as rn
                                from ref_estimates_gas_internal) ind_aq
                               on ind_aq.rn = 1 and
                                  su.external_id = ind_aq.account_id and
                                  mp.meterpointnumber = ind_aq.mprn and
                                  reg.registers_registerreference = ind_aq.register_id and
                                  mt.meterserialnumber = ind_aq.serial_number --TODO: This join needs to be through meterpoint ref number, serial number etc. Register id in regi isn't the same field as everywhere else
           ) reg_level
      group by account_id, registration_id
     ) acc_aggs
         left join (select account_id, max(meterreadingdatetime) as max_read_datetime
                    from ref_readings_internal_valid
                    group by account_id) latest_readings
                   on latest_readings.account_id = acc_aggs.account_id
         left join ref_cdb_registrations creg on acc_aggs.registration_id = creg.id
         left join ref_cdb_quotes q on q.id = creg.quote_id
order by account_id

select *
from ref_cdb_supply_contracts
where external_id = 58955
select *
from ref_cdb_user_permissions
where permissionable_id = 58955
select *
from ref_cdb_users
where id = 59527

select external_id, count(*) as cnt
from ref_cdb_supply_contracts
group by external_id
having cnt > 1

-- Look into account 58955 - the only one where there is a reading, mp details present, not occupier account & no row in cons acc gas
-- 58955 is duplicated. This is the only such case.

select *
from ref_cdb_supply_contracts su
         inner join ref_cdb_addresses addr on su.supply_address_id = addr.id
         inner join ref_cdb_user_permissions up on su.id = up.permissionable_id and permission_level = 0
    and permissionable_type = 'App\\SupplyContract'
         inner join ref_cdb_users u on u.id = up.user_id
where su.external_id = 58955

select *
from ref_meterpoints_raw
where account_id in (38044, 46605, 46606)


-- Live meterpoints lacking meter details
select *
from (select distinct account_id
      from ref_meterpoints_raw
      where (end_date > getdate() or end_date is null)
        and meterpointtype = 'E'
        and start_date <= getdate()) elec_live
         left join ref_consumption_accuracy_elec cons_acc
                   on elec_live.account_id = cons_acc.account_id
         left join (select account_id, count(*) as cnt
                    from ref_readings_internal_valid
                    where meterpointtype = 'E'
                    group by account_id) num_valid_reads
                   on elec_live.account_id = num_valid_reads.account_id
         left join (select sc.external_id,
                           mp.account_id is not null  as mp_present,
                           rm.account_id is not null  as rm_present,
                           reg.account_id is not null as reg_present
                    from ref_cdb_supply_contracts sc
                             left join ref_meterpoints mp on sc.external_id = mp.account_id and mp.meterpointtype = 'E'
                             left join ref_meters rm
                                       on rm.meter_point_id = mp.meter_point_id and rm.account_id = mp.account_id and
                                          rm.removeddate is null
                             left join ref_registers reg
                                       on reg.meter_id = rm.meter_id and reg.account_id = rm.account_id) meter_details
                   on meter_details.external_id = elec_live.account_id
where cons_acc.account_id is null
order by elec_live.account_id


select mp.account_id, mp.meterpointnumber, mp.meterpointtype
from ref_meterpoints mp
         left join ref_meters met on mp.account_id = met.account_id and mp.meter_point_id = met.meter_point_id and met.removeddate is null
where (least(supplyenddate, associationenddate) > getdate() or (supplyenddate is null and associationenddate is null))
  and greatest(associationstartdate, supplystartdate) <= getdate() -- meterpoint is live
  and met.account_id is null

select * from ref_meterpoints where account_id = 1863
select * from ref_meters where meter

select * from ref_meterpoints where account_id in (57191,57211,57327,58468,58572,58805,59059,59851,60156,60660,60704) and meterpointtype = 'G'


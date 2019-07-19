with joined_audits as (select account_id, etlchange, meterreadingsourceuid as update_source
                       from ref_readings_internal_nrl
                       union
                       select rriv.account_id,
                              rria.etlchange,
                              case when rriv.meterpointtype = 'E' then 'RRIV_ELEC' else 'RRIV_GAS' end as update_source
                       from ref_readings_internal_valid rriv
                                left join ref_readings_internal_audit rria
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
)


select *
from ref_consumption_accuracy_elec rcag
         left join (select account_id,
                           max(etlchange)               as most_recent_update_in_period,
                           listagg(update_source, ', ') as updates_in_period,
                           sum(case
                                   when update_source in ('RRIV_ELEC',
                                                          'RRIV_GAS',
                                                          'EST_ELEC',
                                                          'METERPOINTS',
                                                          'METERS',
                                                          'REGISTERS') then 1
                                   else 0 end) >
                           0                            as elec_updated,
                           sum(case
                                   when update_source in ('RRIV_GAS',
                                                          'RRIV_ELEC',
                                                          'NRL_START',
                                                          'NRL_END',
                                                          'EST_GAS',
                                                          'METERPOINTS',
                                                          'METERS',
                                                          'REGISTERS') then 1
                                   else 0 end) >
                           0                            as gas_updated
                    from joined_audits
                    where etlchange between '2019-07-18' and current_timestamp
                    group by account_id
                    order by account_id) acc_updated
                   on acc_updated.account_id = rcag.account_id and
                      acc_updated.elec_updated


where rcag.etlchange > '2019-07-18 00:00:00'
  and acc_updated.account_id is null
order by rcag.account_id
;

select * from ref_readings_internal_audit where account_id = 47195

select *
from ref_consumption_accuracy_elec_audit
where account_id = 3715
order by etlchange desc
select *
from ref_readings_internal_audit
where account_id = 3715
  and meterpointtype = 'E'
order by etlchange desc
select *
from ref_readings_internal_valid
where account_id = 3715
  and meterpointtype = 'E'
order by meterreadingdatetime desc

select count(*) from ref_consumption_accuracy_gas_audit
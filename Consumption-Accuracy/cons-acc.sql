/* elec */

-- insert into ref_consumption_accuracy_elec_temp
select *
-- t.account_id,
-- r.account_id,
-- t.reading_datetime,
-- r.reading_datetime,
-- round(nvl(t.pa_cons_elec, 0), 0) ,
-- round(nvl(r.pa_cons_elec, 0), 0) ,
-- round(nvl(t.igl_ind_eac, 0), 0) ,
-- round(nvl(r.igl_ind_eac, 0), 0) ,
-- round(nvl(t.ind_eac, 0), 0) ,
-- round(nvl(r.ind_eac, 0), 0) ,
-- round(nvl(t.quotes_eac, 0), 0) ,
-- round(nvl(r.quotes_eac, 0), 0)
from (
select
    x.account_id as account_id,
    x.latest_reading_datetime as reading_datetime,
    max(x.pa_cons_elec_acc) as pa_cons_elec,
    max(x.igl_ind_eac_acc) as igl_ind_eac,
    max(x.ind_eac_acc) as ind_eac,
    max(x.quotes_eac_acc) as quotes_eac,
    getdate() as etlchange
from (
select
reads.external_id as account_id,
reads.register_id as register_id,
reads.register_reading_id reading_id,
reads.meterreadingdatetime as reading_datetime,
reads.latest_reading_datetime,
reads.latest_read_per_register,

pa_eac.igloo_eac as pa_cons_elec_reg,
ig_eac.igloo_eac_v1 as igl_ind_eac_reg,
ee.estimation_value as ind_eac_reg,
q.electricity_usage quotes_eac,

sum(pa_eac.igloo_eac) over (partition by reads.external_id, reads.latest_read_per_register) as pa_cons_elec_acc,
sum(ig_eac.igloo_eac_v1) over (partition by reads.external_id, reads.latest_read_per_register) as igl_ind_eac_acc,
sum(ee.estimation_value) over (partition by reads.external_id, reads.latest_read_per_register) as ind_eac_acc,
max(q.electricity_usage) over (partition by reads.external_id, reads.latest_read_per_register) as quotes_eac_acc

from (select su.external_id, su.registration_id, mp.meterpointnumber, reg.register_id, ri.register_reading_id, mt.meterserialnumber, reg.registers_eacaq, reg.registers_registerreference, ri.meterreadingdatetime,
           max(ri.meterreadingdatetime) over (partition by su.external_id) as latest_reading_datetime,
           row_number() over (partition by su.external_id, reg.register_id order by ri.meterreadingdatetime desc) as latest_read_per_register
    from ref_cdb_supply_contracts su
    inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'E'
    inner join ref_meters mt on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and mt.removeddate is null
    inner join ref_registers reg on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
    inner join ref_readings_internal_valid ri on ri.account_id = su.external_id and ri.register_id = reg.register_id
   ) reads
left outer join ref_calculated_eac_audit pa_eac on pa_eac.account_id = reads.external_id and reads.register_id = pa_eac.register_id and reads.meterreadingdatetime = pa_eac.read_max_created_date_elec
left outer join ref_calculated_eac_v1_audit ig_eac on ig_eac.account_id = reads.external_id and reads.register_id = ig_eac.register_id and reads.meterreadingdatetime = ig_eac.read_max_datetime_elec
left outer join ref_estimates_elec_internal ee on ee.account_id = reads.external_id and ee.mpan = reads.meterpointnumber and ee.register_id = reads.registers_registerreference and ee.serial_number = reads.meterserialnumber and ee.effective_from = reads.meterreadingdatetime
left outer join ref_cdb_registrations creg on creg.id = reads.registration_id
left outer join ref_cdb_quotes q on q.id = creg.quote_id
where reads.external_id = 27378
) x

where
       x.latest_read_per_register = 1
group by x.account_id, x.latest_reading_datetime
order by x.account_id, x.latest_reading_datetime
) t
--  left outer join ref_consumption_accuracy_elec r on t.account_id = r.account_id
 where t.account_id = 27378
;

select * from ref_registers where register_id=60645

select * from ref_readings_internal_valid where account_id = 17446
select * from ref_estimates_elec_internal where account_id = 38883;

/* gas */
-- create table temp_ref_consumption_accuracy_gas as (
insert into ref_consumption_accuracy_gas_temp
select
x.account_id as account_id,
x.latest_reading_datetime as reading_datetime,
max(x.pa_cons_gas_acc) as pa_cons_gas,
max(x.igl_ind_aq_acc) as igl_ind_aq,
max(x.ind_aq_acc) as ind_aq,
max(x.quotes_aq_acc) as quotes_aq,
getdate() as etlchange
from (
select
reads.external_id as account_id,
reads.register_id as register_id,
reads.register_reading_id reading_id,
reads.meterreadingdatetime as reading_datetime,
reads.latest_reading_datetime,
reads.latest_read_per_register,

pa_aq.igloo_aq as pa_cons_gas_reg,
ig_aq.igloo_aq_v1 as igl_ind_aq_reg,
reads.estimation_value as ind_aq_reg,
q.gas_usage quotes_aq,

sum(pa_aq.igloo_aq) over (partition by reads.external_id, reads.meterreadingdatetime) as pa_cons_gas_acc,
sum(ig_aq.igloo_aq_v1) over (partition by reads.external_id, reads.meterreadingdatetime) as igl_ind_aq_acc,
sum(reads.estimation_value) over (partition by reads.external_id, reads.meterreadingdatetime) as ind_aq_acc,
max(q.gas_usage) over (partition by reads.external_id, reads.meterreadingdatetime) as quotes_aq_acc

      from (select su.external_id, su.registration_id,  mp.meterpointnumber, reg.register_id,  ri.register_reading_id, mt.meterserialnumber, reg.registers_eacaq, reg.registers_registerreference, ri.meterreadingdatetime,
                   max(ri.meterreadingdatetime) over (partition by su.external_id) as latest_reading_datetime,
                   row_number() over (partition by reg.register_id order by ri.meterreadingdatetime desc) as latest_read_per_register,
                   (select top 1 estimation_value
                                                     from ref_estimates_gas_internal eg
                                                     where eg.account_id = ri.account_id
                                                       and ri.meterpointnumber = eg.mprn
                                                       and ri.registerreference = eg.register_id
                                                       and ri.meterserialnumber = eg.serial_number
                                                       and DATEDIFF(days, ri.meterreadingdatetime, eg.effective_from) between 0 and 40
                                                     order by eg.effective_from desc) as estimation_value
            from ref_cdb_supply_contracts su
            inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'G'
            inner join ref_meters mt on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and mt.removeddate is null
            inner join ref_registers reg on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
            inner join ref_readings_internal_valid ri on ri.account_id = su.external_id and ri.register_id = reg.register_id
            ) reads
left outer join ref_calculated_aq_audit pa_aq on pa_aq.account_id = reads.external_id and reads.register_id = pa_aq.register_id and reads.meterreadingdatetime = pa_aq.read_max_created_date_gas
left outer join ref_calculated_aq_v1_audit ig_aq on ig_aq.account_id = reads.external_id and reads.register_id = ig_aq.register_id and reads.meterreadingdatetime = ig_aq.read_max_datetime_gas
-- left outer join ref_estimates_gas_internal ee on ee.account_id = reads.external_id and ee.mprn = reads.meterpointnumber and ee.register_id = reads.registers_registerreference and ee.serial_number = reads.meterserialnumber and ee.effective_from = reads.meterreadingdatetime
left outer join ref_cdb_registrations creg on creg.id = reads.registration_id
left outer join ref_cdb_quotes q on q.id = creg.quote_id
) x

where
--         x.account_id = 1895
         x.latest_read_per_register = 1
group by x.account_id, x.latest_reading_datetime
order by x.account_id, x.latest_reading_datetime
;


-- Analysis
select * from ref_calculated_eac_v1_audit where account_id = 1831;
select * from ref_calculated_eac_audit where account_id = 1895;

select * from ref_calculated_aq_v1_audit where account_id = 1835;
select * from ref_calculated_aq_audit where account_id = 1835;

select account_id, meterpointtype, count(*) from ref_meterpoints mp
where mp.supplyenddate is null
group by account_id, meterpointtype
having count(*)>1;


alter table ref_calculated_eac_audit owner to igloo_dw_uat_user
alter table ref_calculated_eac_v1_audit owner to igloo_dw_uat_user
alter table ref_calculated_aq_v1_audit owner to igloo_dw_uat_user
alter table ref_calculated_aq_audit owner to igloo_dw_uat_user
;

select * from ref_estimates_elec_internal where account_id = 1895
select * from ref_estimates_gas_internal where account_id = 1895


(select top 1 estimation_value
                                                     from ref_estimates_gas_internal eg
                                                     where eg.account_id = ri.account_id
                                                       and ri.meterpointnumber = eg.mprn
                                                       and ri.registerreference = eg.register_id
                                                       and ri.meterserialnumber = eg.serial_number
                                                       and DATEDIFF(days, ri.meterreadingdatetime, eg.effective_from) between 0 and 40
                                                     order by eg.effective_from desc) as estimation_value


select t.*,
                            case when r.account_id is null then 'n' else 'u' end as etlchangetype,
                            current_timestamp as etlchange
                        from ref_consumption_accuracy_elec t
                       left outer join ref_consumption_accuracy_elec_audit r on t.account_id = r.account_id and t.reading_datetime = r.reading_datetime
                       where  t.pa_cons_elec != r.pa_cons_elec or t.igl_ind_eac != r.igl_ind_eac or t.ind_eac != r.ind_eac or t.quotes_eac != r.quotes_eac
                          or r.account_id is null
/* elec */
-- create table ref_consumption_accuracy_elec as
select
      x.account_id as account_id,
      x.reading_datetime as reading_datetime,
      max(x.pa_cons_elec_acc) pa_cons_elec,
      max(x.igl_ind_eac_acc) igl_ind_eac,
      max(x.ind_eac_acc) ind_eac,
      max(x.quotes_eac_acc) quotes_eac
from (
select
reads.external_id as account_id,
reads.register_id as register_id,
reads.register_reading_id reading_id,
reads.meterreadingdatetime as reading_datetime,
--
pa_eac.igloo_eac as pa_cons_elec_reg,
ig_eac.igloo_eac_v1 as igl_ind_eac_reg,
ee.estimation_value as ind_eac_reg,
q.electricity_usage quotes_eac,

sum(pa_eac.igloo_eac) over (partition by reads.external_id, reads.meterreadingdatetime) as pa_cons_elec_acc,
sum(ig_eac.igloo_eac_v1) over (partition by reads.external_id, reads.meterreadingdatetime) as igl_ind_eac_acc,
sum(ee.estimation_value) over (partition by reads.external_id, reads.meterreadingdatetime) as ind_eac_acc,
max(q.electricity_usage) over (partition by reads.external_id, reads.meterreadingdatetime) as quotes_eac_acc

from (select su.external_id, su.registration_id, mp.meterpointnumber, reg.register_id,  ri.register_reading_id,mt.meterserialnumber, reg.registers_eacaq, reg.registers_registerreference, ri.meterreadingdatetime
      from ref_cdb_supply_contracts su
      inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'E'
      inner join ref_meters mt on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and mt.removeddate is null
      inner join ref_registers reg on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
      inner join ref_readings_internal_valid ri on ri.account_id = su.external_id and ri.register_id = reg.register_id
     ) reads
left outer join ref_calculated_eac_audit pa_eac on pa_eac.account_id = reads.external_id and reads.register_id = pa_eac.register_id and reads.meterreadingdatetime = pa_eac.read_max_created_date_elec
left outer join ref_calculated_eac_v1_audit ig_eac on ig_eac.account_id = reads.external_id and reads.register_id = ig_eac.register_id and reads.meterreadingdatetime = ig_eac.read_max_datetime_elec and ig_eac.etlchangetype = 'n'
left outer join ref_estimates_elec_internal ee on ee.account_id = reads.external_id and ee.mpan = reads.meterpointnumber and ee.register_id = reads.registers_registerreference and ee.serial_number = reads.meterserialnumber and ee.effective_from = reads.meterreadingdatetime
left outer join ref_cdb_registrations creg on creg.id = reads.registration_id
left outer join ref_cdb_quotes q on q.id = creg.quote_id
) x

where
    x.account_id = 1895 and
(x.pa_cons_elec_reg is not null or x.igl_ind_eac_reg is not null)
group by x.account_id, x.reading_datetime
order by x.account_id, x.reading_datetime
;


/* gas */
-- create table ref_consumption_accuracy_gas as
select
      x.account_id as account_id,
      x.reading_datetime as reading_datetime,
      max(x.pa_cons_gas_acc) pa_cons_gas,
      max(x.igl_ind_aq_acc) as igl_ind_aq,
      max(x.ind_aq_acc) as ind_aq,
      max(x.quotes_aq_acc) as quotes_aq
from (
select
reads.external_id as account_id,
reads.register_id as register_id,
reads.register_reading_id reading_id,
reads.meterreadingdatetime as reading_datetime,

pa_aq.igloo_aq as pa_cons_gas_reg,
ig_aq.igloo_aq_v1 as igl_ind_aq_reg,
ee.estimation_value as ind_aq_reg,
q.gas_usage quotes_aq,

sum(pa_aq.igloo_aq) over (partition by reads.external_id, reads.meterreadingdatetime) as pa_cons_gas_acc,
sum(ig_aq.igloo_aq_v1) over (partition by reads.external_id, reads.meterreadingdatetime) as igl_ind_aq_acc,
sum(ee.estimation_value) over (partition by reads.external_id, reads.meterreadingdatetime) as ind_aq_acc,
max(q.gas_usage) over (partition by reads.external_id, reads.meterreadingdatetime) as quotes_aq_acc

from (select su.external_id, su.registration_id,  mp.meterpointnumber, reg.register_id,  ri.register_reading_id, mt.meterserialnumber, reg.registers_eacaq, reg.registers_registerreference, ri.meterreadingdatetime
      from ref_cdb_supply_contracts su
      inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'G'
      inner join ref_meters mt on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and mt.removeddate is null
      inner join ref_registers reg on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
      inner join ref_readings_internal_valid ri on ri.account_id = su.external_id and ri.register_id = reg.register_id
     ) reads
left outer join ref_calculated_aq_audit pa_aq on pa_aq.account_id = reads.external_id and reads.register_id = pa_aq.register_id and reads.meterreadingdatetime = pa_aq.read_max_created_date_gas
left outer join ref_calculated_aq_v1_audit ig_aq on ig_aq.account_id = reads.external_id and reads.register_id = ig_aq.register_id and reads.meterreadingdatetime = ig_aq.read_max_datetime_gas
left outer join ref_estimates_gas_internal ee on ee.account_id = reads.external_id and ee.mprn = reads.meterpointnumber and ee.register_id = reads.registers_registerreference and ee.serial_number = reads.meterserialnumber and ee.effective_from = reads.meterreadingdatetime
left outer join ref_cdb_registrations creg on creg.id = reads.registration_id
left outer join ref_cdb_quotes q on q.id = creg.quote_id
) x

where
--     x.account_id = 1831 and
(x.pa_cons_gas_reg is not null or x.igl_ind_aq_reg is not null)
group by x.account_id, x.reading_datetime
order by x.account_id, x.reading_datetime
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
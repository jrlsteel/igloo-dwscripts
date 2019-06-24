-- Insert into eac
insert into ref_calculated_eac
select account_id,
elec_gsp,
elec_ssc,
meterpoint_id,
supplyend_date,
meter_removed_date,
register_id,
no_of_digits,
read_min_created_date_elec,
read_max_created_date_elec,
read_min_readings_elec,
read_max_readings_elec,
read_months_diff_elec,
read_days_diff_elec,
read_consumption_elec,
ppc,
industry_eac,
igloo_eac,
getdate() as etlchange
from ref_calculated_eac e1 where e1.account_id = 1838;

-- Insert into eac_v1
insert into ref_calculated_eac_v1
select account_id,
elec_gsp,
elec_ssc,
meterpoint_id,
supplyend_date,
meter_removed_date,
register_id,
no_of_digits,
read_min_datetime_elec,
read_max_datetime_elec,
read_min_readings_elec,
read_max_readings_elec,
read_months_diff_elec,
read_days_diff_elec,
no_of_ppc_rows,
no_of_bpp_rows,
read_consumption_elec,
profile_class,
tpr,
ppc,
bpp,
sp,
total_reads,
industry_eac_register,
previous_ind_eac_estimates,
latest_ind_eac_estimates,
igloo_eac_v1,
getdate() as etlchange
from ref_calculated_eac_v1 e1 where e1.account_id = 1838;


insert into ref_calculated_aq
select account_id,
gas_ldz,
gas_imperial_meter_indicator,
meterpoint_id,
supplyend_date,
meter_removed_date,
register_id,
no_of_digits,
read_min_created_date_gas,
read_max_created_date_gas,
read_min_readings_gas,
read_max_readings_gas,
read_months_diff_gas,
read_days_diff_gas,
read_consumption_gas,
cv,
waalp,
rmq,
industry_aq,
igloo_aq,
getdate() as etlchange
from ref_calculated_aq e1 where e1.account_id = 1838;


insert into ref_calculated_aq_v1
select account_id,
gas_ldz,
gas_imperial_meter_indicator,
meterpoint_id,
supplyend_date,
meter_removed_date,
register_id,
no_of_digits,
read_min_datetime_gas,
read_max_datetime_gas,
read_min_readings_gas,
read_max_readings_gas,
read_months_diff_gas,
read_days_diff_gas,
read_consumption_gas,
cv,
waalp,
rmq,
industry_aq_on_register,
industry_aq_on_estimates,
u,
igloo_aq_v1,
getdate() as etlchange
from ref_calculated_aq_v1 e1 where e1.account_id = 1838;


select * from ref_calculated_eac where account_id = 1838;
etlchange
2019-05-23 15:32:31.000000
2019-05-23 15:49:30.000000

select * from ref_calculated_eac_v1 where account_id = 1838;

select * from ref_calculated_aq where account_id = 1838;
etlchange
2019-05-23 15:33:06.000000
2019-05-23 15:49:31.000000

select * from ref_calculated_aq_v1 where account_id = 1838;

select * from ref_calculated_eac_audit where account_id = 1838

select * from ref_consumption_accuracy_elec where account_id = 1838;

select * from ref_consumption_accuracy_gas where account_id = 1838;

insert into ref_consumption_accuracy_elec
select
account_id,
reading_datetime,
pa_cons_elec,
igl_ind_eac,
ind_eac,
quotes_eac,
getdate() as etlchange
from ref_consumption_accuracy_elec e1 where e1.account_id = 1838;

insert into ref_consumption_accuracy_gas
select
account_id,
reading_datetime,
pa_cons_gas,
igl_ind_aq,
ind_aq,
quotes_aq,
getdate() as etlchange
from ref_consumption_accuracy_gas e1 where e1.account_id = 1838;


select etlchange,  count(*) as eac from ref_calculated_eac group by etlchange; --0
select etlchangetype, etlchange, count(*) as eac_audit from ref_calculated_eac_audit group by etlchangetype, etlchange; --0

select etlchange, count(*) as aq from ref_calculated_aq group by etlchange; --0
select etlchangetype, etlchange, count(*) as aq_audit from ref_calculated_aq_audit group by etlchangetype, etlchange; --0

select etlchange, count(*) as eac_v1 from ref_calculated_eac_v1 group by etlchange; --0
select etlchangetype, etlchange, count(*) as eac_v1_audit from ref_calculated_eac_v1_audit group by etlchangetype, etlchange; --0

select etlchange, count(*) as aq_v1 from ref_calculated_aq_v1 group by etlchange; --0
select etlchangetype, etlchange, count(*) as aq_v1_audit from ref_calculated_aq_v1_audit group by etlchangetype, etlchange; --0

select etlchange, count(*) as ca_elec from ref_consumption_accuracy_elec group by etlchange; --0
select etlchangetype, etlchange, count(*) as ca_elec_audit from ref_consumption_accuracy_elec_audit group by etlchangetype, etlchange; --0

select etlchange, count(*) as ca_gas from ref_consumption_accuracy_gas group by etlchange; --0
select etlchangetype, etlchange, count(*) as ca_gas_audit from ref_consumption_accuracy_gas_audit group by etlchangetype, etlchange; --0

select e.*, e1.* from ref_calculated_eac_v1_audit e
left outer join ref_calculated_eac_v1 e1 on e1.account_id = e.account_id and e1.register_id = e.register_id and e1.etlchange = '2019-05-29 13:18:29.931000'
where e.etlchange = '2019-05-29 13:17:16.318000' and e1.account_id is null

select * from ref_registers where account_id = 12474

select count(*) from ref_readings_internal_valid_audit group by etlchangetype;

select * from ref_consumption_accuracy_elec_audit where account_id = 38883;
select * from ref_estimates_elec_internal where account_id = 1858;

select account_id from ref_consumption_accuracy_elec_audit where etlchangetype = 'n'
group by account_id
having count(*) > 1;

select * from ref_consumption_accuracy_elec where etlchange='2019-05-28 15:28:08.019000'

select count(*)
from ref_consumption_accuracy_elec_audit t
left outer join ref_consumption_accuracy_elec r on t.account_id = r.account_id and r.etlchange = '2019-05-28 13:58:34.971000'
where t.etlchange = '2019-05-28 13:55:05.146000' and r.account_id is null
-- t.reading_datetime != r.reading_datetime
--     or r.account_id is null;

select account_id, count(*) from ref_meterpoints
where meterpointtype = 'E' and (supplyenddate is null)
group by account_id
having count(*) > 2;

-- delete from ref_consumption_accuracy_elec_temp;
select count(*) from ref_consumption_accuracy_elec_temp-- 32070
select count(*) from ref_consumption_accuracy_elec_audit -- 32070
select count(*) from ref_consumption_accuracy_elec -- 32070


select * from (
select t.account_id, t.reading_datetime,
       t.pa_cons_elec, t.igl_ind_eac, t.ind_eac, t.quotes_eac,
              r.*,
                            case when r.account_id is null then 'n' else 'u' end as etlchangetype,
                            current_timestamp as etlchange
                        from ref_consumption_accuracy_elec_temp t
                       left outer join ref_consumption_accuracy_elec r on t.account_id = r.account_id and t.reading_datetime = r.reading_datetime
                       where (round(nvl(t.pa_cons_elec, 0), 0) != round(nvl(r.pa_cons_elec, 0), 0) or round(nvl(t.igl_ind_eac, 0), 0) != round(nvl(r.igl_ind_eac, 0), 0) or round(nvl(t.ind_eac, 0), 0) != round(nvl(r.ind_eac, 0), 0) or round(nvl(t.quotes_eac, 0), 0) != round(nvl(r.quotes_eac, 0), 0)
                           or r.account_id is null)
                           and r.account_id = 27378
) x; -- 32070




-- audit_elec
select count(*) from ref_consumption_accuracy_elec_audit; --0

-- insert into ref_consumption_accuracy_elec_audit
select t.account_id, t.reading_datetime,
       t.pa_cons_elec, t.igl_ind_eac, t.ind_eac, t.quotes_eac,
                            case when r.account_id is null then 'n' else 'u' end as etlchangetype,
                            current_timestamp as etlchange
                        from ref_consumption_accuracy_elec_temp t
                       left outer join ref_consumption_accuracy_elec r on t.account_id = r.account_id and t.reading_datetime = r.reading_datetime
                       where  (round(coalesce(t.pa_cons_elec, 0), 0) != round(coalesce(r.pa_cons_elec, 0), 0) or round(coalesce(t.igl_ind_eac, 0), 0) != round(coalesce(r.igl_ind_eac, 0), 0) or round(coalesce(t.ind_eac, 0), 0) != round(coalesce(r.ind_eac, 0), 0) or round(coalesce(t.quotes_eac, 0), 0) != round(coalesce(r.quotes_eac, 0), 0)
                          or r.account_id is null)
                          and r.account_id = 27378

select count(*) from ref_consumption_accuracy_elec_audit; --32070


insert into ref_consumption_accuracy_elec
select t.account_id, t.reading_datetime,
       t.pa_cons_elec, t.igl_ind_eac, t.ind_eac, t.quotes_eac,
--                             case when r.account_id is null then 'n' else 'u' end as etlchangetype,
                            current_timestamp as etlchange
                        from ref_consumption_accuracy_elec_temp t
                       left outer join ref_consumption_accuracy_elec r on t.account_id = r.account_id and t.reading_datetime = r.reading_datetime
                       where  round(t.pa_cons_elec, 0) != round(r.pa_cons_elec, 0) or round(t.igl_ind_eac, 0) != round(r.igl_ind_eac, 0) or round(t.ind_eac, 0) != round(r.ind_eac, 0) or round(t.quotes_eac, 0) != round(r.quotes_eac, 0)
                          or r.account_id is null

select count(*) from ref_consumption_accuracy_elec; --32070


delete from ref_consumption_accuracy_elec
                                        using (select x.account_id, x.etlchange from (select e.*,
                                                        row_number() over (partition by account_id order by etlchange desc) as rn
                                                    from ref_consumption_accuracy_elec e) x where x.rn > 1) x1
                                                    where x1.account_id = ref_consumption_accuracy_elec.account_id and x1.etlchange = ref_consumption_accuracy_elec.etlchange
;

select * from ref_consumption_accuracy_elec where account_id = 27378
select * from ref_consumption_accuracy_elec_temp where account_id = 27378
select * from ref_consumption_accuracy_elec_audit where account_id = 17446
select * from ref_estimates_elec_internal where account_id = 17446
select * from ref_estimates_elec_internal_audit where account_id = 17446

select * from ref_calculated_eac_audit where account_id = 17446;
select * from ref_calculated_eac_v1_audit where account_id = 17446;

select * from ref_cdb_supply_contracts su
left outer join ref_cdb_registrations creg on creg.id = su.registration_id
left outer join ref_cdb_quotes q on q.id = creg.quote_id
where external_id = 17446;

select * from ref_calculated_aq where account_id = 17446;
select * from ref_calculated_aq_v1 where account_id = 17446;

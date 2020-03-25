/*
 1) Check non-cancelled customers have a tariff id
 Expectation: Empty result set
 */
select *
from ref_calculated_tariff_accounts
WHERE tariff_id is null
  and account_id in (
    select distinct(dcf.account_id)
    from public.ref_calculated_tariff_accounts ta
             full join public.ref_calculated_daily_customer_file dcf on ta.account_id = dcf.account_id
    where ta.tariff_id is null
      and dcf.account_status != 'Cancelled'
);

/*
 2) Check if elec customers have the appropriate tariff for Live and Pending-Final
 Expectation: count=0
 */
SELECT COUNT(*) as non_matching_electric_tariff
from (
         select ta.account_id, ta.tariff_id, t.id, t.fuel_type, dcf.account_id as rec_id, dcf.elec_reg_status
         from (select *
               from ref_calculated_daily_customer_file
               where lower(elec_reg_status) in ('live', 'pending final')) dcf
                  right join (select *
                              from ref_calculated_tariff_accounts
                              where account_id not in (select distinct account_id
                                                       from ref_calculated_daily_customer_file
                                                       where lower(elec_reg_status) not in ('live', 'pending final'))) ta
                             on dcf.account_id = ta.account_id
                  left join ref_tariffs t on ta.tariff_id = t.id
         where t.fuel_type = 'E'
     ) qry1
where nvl(qry1.account_id, 8888) <> nvl(qry1.rec_id, 9999);

/*
 3) Check if gas customers have the appropriate tariff for Live and Pending-Final
 Expectation: count=0
 */
SELECT COUNT(*) as non_matching_gas_tariff
from (
         select ta.account_id, ta.tariff_id, t.id, t.fuel_type, dcf.account_id as rec_id, dcf.gas_reg_status
         from (select *
               from ref_calculated_daily_customer_file
               where lower(gas_reg_status) in ('live', 'pending final')) dcf
                  right join (select *
                              from ref_calculated_tariff_accounts
                              where account_id not in (select distinct account_id
                                                       from ref_calculated_daily_customer_file
                                                       where lower(gas_reg_status) not in ('live', 'pending final'))) ta
                             on dcf.account_id = ta.account_id
                  left join ref_tariffs t on ta.tariff_id = t.id
         where t.fuel_type = 'G'
     ) qry1
where nvl(qry1.account_id, 8888) <> nvl(qry1.rec_id, 9999);

/*
 4) Check accounts have the correct number of live tariffs (1 for single-fuel, 2 for dual fuel)
 Expectation: Empty result set
 */
select dcf.account_id,
       max((dcf.elec_reg_status in ('Live', 'Pending Final'))::int) as elec_reg,
       max((dcf.gas_reg_status in ('Live', 'Pending Final'))::int)  as gas_reg,
       sum((nvl(t.fuel_type, '') = 'E')::int)                       as elec_tariffs,
       sum((nvl(t.fuel_type, '') = 'G')::int)                       as gas_tariffs
from ref_tariffs t
         right join ref_calculated_tariff_accounts ta on ta.tariff_id = t.id
         right join ref_calculated_daily_customer_file dcf on dcf.account_id = ta.account_id and
                                                              (ta.end_date is null or
                                                               (t.fuel_type = 'E' and ta.end_date >= dcf.elec_ed) or
                                                               (t.fuel_type = 'G' and ta.end_date >= dcf.gas_ed))
where dcf.account_status in ('Live', 'Pending Final')
group by dcf.account_id
having elec_reg > elec_tariffs
    or gas_reg > gas_tariffs
    or gas_tariffs > 1
    or elec_tariffs > 1
order by account_id;

/*
 5) Check number of error codes is reasonable
 Expectation:
 error_code	num_records	num_accounts
Live_Valid	154633	89719
Final_Valid	21834	12689
Pending_Valid	2575	2072
FINAL_MISMATCH	704	426
LIVE_ENSEK_MISSING	46	34
FINAL_ENSEK_MISSING	15	9
LIVE_MISMATCH	6	4
PENDING_ENSEK_MISSING	5	5
FINAL_IGLOO_MISSING	2	1
 */
select error_code, count(*) as num_records, count(distinct account_id) as num_accounts
from vw_tariff_checks
group by error_code
order by count(*) desc

select *
from ref_tariff_history_elec_ur
where account_id = 128242


select distinct account_id
from vw_tariff_checks
where error_code in ('LIVE_ENSEK_MISSING', 'LIVE_MISMATCH',
                     'PENDING_ENSEK_MISSING', 'PENDING_MISMATCH')

select *
from ref_meterpoints_audit
where account_id = 128242

select dcf.account_id, gsp, elec_ssd, gas_ssd, elec_ed, gas_ed
from ref_calculated_daily_customer_file dcf
left join vw_tariff_checks tc on dcf.account_id = tc.account_id
where tc.error_code in ('PENDING_ENSEK_MISSING', 'LIVE_MISMATCH', 'PENDING_MISMATCH')

select * from ref_tariffs where end_date is null-- and gsp_ldz = '_G'

select * from ref_meterpoints_attributes where account_id = 14134
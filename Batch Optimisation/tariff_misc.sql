create or replace view vw_tariffs_combined as
select tariffs_elec.id,
       tariffs_elec.gsp_ldz,
       tariffs_elec.name,
       tariffs_elec.billing_start_date,
       tariffs_elec.signup_start_date,
       tariffs_elec.end_date,
       tariffs_elec.standing_charge as elec_standing_charge,
       tariffs_elec.unit_rate       as elec_unit_rate,
       tariffs_gas.standing_charge  as gas_standing_charge,
       tariffs_gas.unit_rate        as gas_unit_rate,
       tariffs_elec.discounts,
       tariffs_elec.tariff_type,
       tariffs_elec.exit_fees
from (select * from ref_tariffs where fuel_type = 'E') tariffs_elec
         left join (select * from ref_tariffs where fuel_type = 'G') tariffs_gas
                   on tariffs_elec.gsp_ldz = tariffs_gas.gsp_ldz and
                      tariffs_elec.billing_start_date = tariffs_gas.billing_start_date and
                      tariffs_elec.signup_start_date = tariffs_gas.signup_start_date and
                      nvl(tariffs_elec.end_date, '2000-01-01') =
                      nvl(tariffs_gas.end_date, '2000-01-01')

select *
from ref_cdb_supply_contracts
where external_id = 1830

select max(account_id)
from vw_customer_file

select floor(cth.account_id / 1000.0) as thousand, count(*) as cnt
from vw_compare_tariff_histories cth
         left join vw_customer_file cf on cf.account_id = cth.account_id
where cf.account_status != 'Cancelled'
  and cf.home_move_in = false
group by thousand
order by thousand


select *
from ref_cdb_supply_contracts
where external_id = 25027

select *
from ref_meterpoints
where account_id = 25027

select wl0_date
from vw_customer_file
where account_id = 25027

select cth.*
from vw_compare_tariff_histories cth
         left join vw_customer_file cf on cf.account_id = cth.account_id
where cf.account_status != 'Cancelled'
  and cf.home_move_in = false
  and cth.account_id between 32000 and 34000

select cf.account_id,
       elec_ssd,
       gas_ssd,
       th.start_date,
       sc.external_id,
       sc.created_at,
       signup_start_date,
       billing_start_date
from vw_customer_file cf
         left join ref_cdb_supply_contracts sc on cf.account_id = sc.external_id
         left join (select account_id, min(start_date) as start_date from ref_tariff_history group by account_id) th
                   on cf.account_id = th.account_id
         left join (select distinct signup_start_date, billing_start_date from ref_tariffs) windows
                   on th.start_date >= signup_start_date and th.start_date < billing_start_date
where cf.home_move_in
--  and start_date != created_at
  and windows.billing_start_date is not null
order by account_id desc

select * from ref_tariff_history where account_id = 50066

select distinct signup_start_date, billing_start_date from ref_tariffs


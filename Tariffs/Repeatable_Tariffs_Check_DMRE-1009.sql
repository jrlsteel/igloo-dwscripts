-- Req's and error codes:
/*
1) for all live & pending final fuels (by account-fuel pair)
    A) a tariff must exist in I with end date null or in the future - LIVE_IGLOO_MISSING
    B) a tariff must exist in E with end date null or in the future - LIVE_ENSEK_MISSING
    C) the values in these tariffs must match - LIVE_MISMATCH
2) for all pending live fuels (by account-fuel pair)
    A) a tariff must exist in I dataset with tariff start date <= fuel start date - PENDING_IGLOO_MISSING
    B) if fuel ssd is within 14 days a tariff must exist in E dataset with start date <= fuel start date - 
        PENDING_ENSEK_MISSING
    C) where a tariff exists in both E and I for this fuel, the tariffs should match - PENDING_MISMATCH
3) for all final fuels (by account-fuel pair)
    A) a tariff must exist in I with tariff start < fuel end and tariff end null or >= fuel end - FINAL_IGLOO_MISSING
    B) a tariff must exist in E with tariff start < fuel end and tariff end null or >= fuel end - FINAL_ENSEK_MISSING
    C) the values in these tariffs should match - FINAL_MISMATCH
*/
create view vw_tariff_checks as
    with latest_ensek as
             (select *
              from (select *, row_number() over (partition by account_id, fuel order by start_date desc) as rn
                    from (select eur.account_id,
                                 'E'                   as fuel,
                                 esc.rate              as standing_charge,
                                 eur.rate              as unit_rate,
                                 trunc(eur.start_date) as start_date,
                                 trunc(eur.end_date)   as end_date
                          from ref_tariff_history_elec_ur eur
                                   inner join ref_tariff_history_elec_sc esc on eur.account_id = esc.account_id and
                                                                                eur.start_date = esc.start_date
                                   inner join ref_calculated_daily_customer_file dcf
                                              on eur.account_id = dcf.account_id and
                                                 eur.start_date <= nvl(dcf.elec_ed, getdate() + 1000)
                          union
                          select gur.account_id,
                                 'G'                   as fuel,
                                 gsc.rate              as standing_charge,
                                 gur.rate              as unit_rate,
                                 trunc(gur.start_date) as start_date,
                                 trunc(gur.end_date)   as end_date
                          from ref_tariff_history_gas_ur gur
                                   inner join ref_tariff_history_gas_sc gsc on gur.account_id = gsc.account_id and
                                                                               gur.start_date = gsc.start_date
                                   inner join ref_calculated_daily_customer_file dcf
                                              on gur.account_id = dcf.account_id and
                                                 gur.start_date <= nvl(dcf.gas_ed, getdate() + 1000)
                         ) ensek_tariffs
                   ) numbered_ensek_tariffs
              where rn = 1),
         latest_igloo as
             (select *
              from (select ta.account_id,
                           t.fuel_type                                                                        as fuel,
                           t.standing_charge,
                           t.unit_rate,
                           trunc(ta.start_date)                                                               as start_date,
                           trunc(ta.end_date)                                                                 as end_date,
                           row_number() over (partition by account_id, fuel_type order by ta.start_date desc) as rn
                    from ref_calculated_tariff_accounts ta
                             inner join ref_tariffs t on ta.tariff_id = t.id) numbered_igloo_tariffs
              where rn = 1),
         account_fuel_pairs as
             (select account_id,
                     'E'             as fuel,
                     trunc(elec_ssd) as ssd,
                     trunc(elec_ed)  as sed,
                     elec_reg_status as reg_status
              from ref_calculated_daily_customer_file
              where elec_reg_status is not null
                and elec_reg_status != 'Cancelled'
              union
              select account_id,
                     'G'            as fuel,
                     trunc(gas_ssd) as ssd,
                     trunc(gas_ed)  as sed,
                     gas_reg_status as reg_status
              from ref_calculated_daily_customer_file
              where gas_reg_status is not null
                and gas_reg_status != 'Cancelled')
    select afp.*,
           igl_trf.standing_charge is null or igl_trf.unit_rate is null                                 as igl_trf_missing,
           igl_trf.standing_charge                                                                      as igl_trf_standing_charge,
           igl_trf.unit_rate                                                                            as igl_trf_unit_rate,
           igl_trf.start_date                                                                           as igl_trf_start_date,
           igl_trf.end_date                                                                             as igl_trf_end_date,
           ens_trf.standing_charge is null or ens_trf.unit_rate is null                                 as ens_trf_missing,
           ens_trf.standing_charge                                                                      as ens_trf_standing_charge,
           ens_trf.unit_rate                                                                            as ens_trf_unit_rate,
           ens_trf.start_date                                                                           as ens_trf_start_date,
           ens_trf.end_date                                                                             as ens_trf_end_date,
           ens_trf.unit_rate != igl_trf.unit_rate or ens_trf.standing_charge != igl_trf.standing_charge as trf_mismatch,
           case
               when afp.reg_status in ('Live', 'Pending Final') then
                   case
                       when igl_trf_missing or nvl(igl_trf.end_date, getdate() + 1) < getdate()
                           then 'LIVE_IGLOO_MISSING'
                       when ens_trf_missing or nvl(ens_trf.end_date, getdate() + 1) < getdate()
                           then 'LIVE_ENSEK_MISSING'
                       when trf_mismatch then 'LIVE_MISMATCH'
                       else 'Live_Valid'
                       end
               when afp.reg_status = 'Pending Live' then
                   case
                       when igl_trf_missing or igl_trf.start_date > afp.ssd
                           then 'PENDING_IGLOO_MISSING'
                       when datediff(days, getdate(), afp.ssd) <= 14 and
                            (ens_trf_missing or ens_trf.start_date > afp.ssd)
                           then 'PENDING_ENSEK_MISSING'
                       when (not ens_trf_missing) and trf_mismatch then 'PENDING_MISMATCH'
                       else 'Pending_Valid'
                       end
               when afp.reg_status = 'Final' then
                   case
                       when igl_trf_missing or
                            afp.sed not between igl_trf.start_date and nvl(igl_trf.end_date, getdate() + 1)
                           then 'FINAL_IGLOO_MISSING'
                       when ens_trf_missing or
                            afp.sed not between ens_trf.start_date and nvl(ens_trf.end_date, getdate() + 1)
                           then 'FINAL_ENSEK_MISSING'
                       when trf_mismatch then 'FINAL_MISMATCH'
                       else 'Final_Valid'
                       end
               else 'REG_STATUS_INVALID'
               end                                                                                      as error_code
    from account_fuel_pairs afp
             left join latest_ensek ens_trf on afp.account_id = ens_trf.account_id and afp.fuel = ens_trf.fuel
             left join latest_igloo igl_trf on afp.account_id = igl_trf.account_id and afp.fuel = igl_trf.fuel
    order by afp.account_id, afp.fuel;


-- Daily API filtering
select distinct account_id
from vw_tariff_checks
where error_code in ('LIVE_ENSEK_MISSING', 'LIVE_MISMATCH',
                     'PENDING_ENSEK_MISSING', 'PENDING_MISMATCH');

-- Weekly API filtering
select distinct account_id
from vw_tariff_checks
where error_code in ('LIVE_ENSEK_MISSING', 'LIVE_MISMATCH',
                     'PENDING_ENSEK_MISSING', 'PENDING_MISMATCH',
                     'FINAL_ENSEK_MISSING', 'FINAL_MISMATCH');


select error_code,
       count(*)                           as num_records,
       count(distinct ttd.account_id)     as num_accounts,
       count(distinct bs_accs.account_id) as num_bs_accs
from temp_tariffs_diffs ttd
         left join temp_account_list bs_accs on ttd.account_id = bs_accs.account_id
group by error_code
order by count(*) desc

create table temp_tariffs_diffs_5th_Feb as
select *
from temp_tariffs_diffs

select account_id, occupier_account, home_move_in
from ref_calculated_daily_customer_file
where account_id in (
    select distinct account_id from temp_tariffs_diffs where error_code = 'FINAL_IGLOO_MISSING'-- and not ens_trf_present
)
order by account_id

select fuel, count(*)
from temp_tariffs_diffs
where error_code = 'Pending_Valid'
group by fuel
select *
from temp_tariffs_diffs

select account_id, listagg(fuel) as fuels
from temp_tariffs_diffs
where error_code = 'Pending_Valid'
group by account_id
having fuels = 'EG'
    or fuels = 'GE'

select supply_type, count(*)
from ref_calculated_daily_customer_file
where account_status = 'Pending Live'
group by supply_type

select *
from temp_tariffs_diffs
where right(error_code, 13) = 'ENSEK_MISSING'
--and not ens_trf_present

select *
from ref_calculated_tariff_accounts ta
         left join ref_tariffs t on ta.tariff_id = t.id
where account_id = 46001
--   and ta.end_date is null
/*
1) for all live & pending final fuels (by account-fuel pair)
    A) a tariff must exist in I with end date null or in the future - LIVE_IGLOO_MISSING
    B) a tariff must exist in E with end date null or in the future - LIVE_ENSEK_MISSING
    C) the values in these tariffs must match - LIVE_MISMATCH
2) for all pending live fuels (by account-fuel pair)
    A) a tariff must exist in I dataset with tariff start date <= fuel start date - PENDING_IGLOO_MISSING
    B) if fuel ssd is within 14 days a tariff must exist in E dataset with start date <= fuel start date - 
        PENDING_ENSEK_MISSING
    C) where a tariff exists in both E and I for this fuel, the tariffs should match - PENDING_MISMATCH
3) for all final fuels (by account-fuel pair)
    A) a tariff must exist in I with tariff start < fuel end and tariff end null or >= fuel end - FINAL_IGLOO_MISSING
    B) a tariff must exist in E with tariff start < fuel end and tariff end null or >= fuel end - FINAL_ENSEK_MISSING
    C) the values in these tariffs should match - FINAL_MISMATCH
*/



select *
from ref_tariff_history_gas_ur
where account_id = 7553
select *
from ref_calculated_daily_customer_file
where account_id = 1945
select *
from ref_calculated_tariff_accounts
where account_id = 1945

select *
from temp_tariffs_diffs
where error_code = 'FINAL_MISMATCH'
order by account_id

select *
from (select account_id,
             'E'             as fuel,
             elec_ssd        as ssd,
             elec_ed         as sed,
             elec_reg_status as reg_status
      from ref_calculated_daily_customer_file
      where elec_reg_status is not null
        and elec_reg_status != 'Cancelled'
      union
      select account_id,
             'G'            as fuel,
             gas_ssd        as ssd,
             gas_ed         as sed,
             gas_reg_status as reg_status
      from ref_calculated_daily_customer_file
      where gas_reg_status is not null
        and gas_reg_status != 'Cancelled')
where account_id = 1945

select *
from temp_calc_tariff_accounts ta /*inner join ref_tariffs t on ta.tariff_id = t.id*/
where account_id = 1945

select signup_date
from ref_calculated_daily_customer_file
where account_id = 1945



select distinct account_id
from temp_tariffs_diffs
where right(error_code, 5) != 'Valid'
order by account_id



select *
from ref_tariff_history_gas_ur
where account_id = 32845

select *
from ref_tariff_history_gas_ur
where account_id = 32845

select distinct account_id
from ref_tariff_history_gas_ur
where rate is null

select *
from aws_s3_stage2_extracts.stage2_tariffhistorygasunitrates
where account_id in (32845, 54977)
select *
from aws_s3_stage2_extracts.stage2_tariffhistorygasunitrates
where account_id = 32845



select *
from temp_tariffs_diffs
where account_id = 54977


select *
from temp_tariffs_diffs
where error_code = 'FINAL_MISMATCH'

create table temp_account_list
(
    account_id bigint
)

select count(account_id), count(distinct account_id)
from temp_account_list
;

select ttd.account_id,
       fuel,
       bs_accs.account_id is not null as billing_suspended,
       ttd.ens_trf_start_date,
       ttd.ens_trf_end_date,
       gas_ssd,
       gas_ed,
       elec_ssd,
       elec_ed
from temp_tariffs_diffs ttd
         left join temp_account_list bs_accs on ttd.account_id = bs_accs.account_id
         left join ref_calculated_daily_customer_file dcf on dcf.account_id = ttd.account_id
where ttd.error_code = 'LIVE_ENSEK_MISSING'
order by account_id


select * from vw_tariff_checks where right(error_code, 5) != 'Valid'
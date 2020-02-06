-- list of all tariffs an account has been on (split into elec / gas)
select dcf.account_id,
       ta.start_date as ta_start,
       ta.end_date   as ta_end,
       t.id          as tariff_id,
       t.fuel_type,
       t.gsp_ldz     as tariff_gsp,
       t.unit_rate,
       t.standing_charge
from ref_calculated_daily_customer_file dcf
         left join ref_calculated_tariff_accounts ta on ta.account_id = dcf.account_id
         left join ref_tariffs t on t.id = ta.tariff_id
where dcf.account_id = 1831
order by account_id, ta_start, tariff_id

-- list of most recent tariffs applied to accounts
select *
from (with most_recent_tariffs as (
    select *
    from (
             select ta.account_id,
                    ta.start_date                                                                      as ta_start,
                    ta.end_date                                                                        as ta_end,
                    t.id                                                                               as tariff_id,
                    t.fuel_type,
                    t.gsp_ldz                                                                          as tariff_gsp,
                    t.unit_rate,
                    t.standing_charge,
                    row_number() over (partition by account_id, fuel_type order by ta.start_date desc) as rn
             from ref_calculated_tariff_accounts ta
                      left join ref_tariffs t on ta.tariff_id = t.id
         ) ordered_tariff_accounts
    where rn = 1
)
      select dcf.account_id,
             dcf.account_status,
             coalesce(mrt_elec.tariff_gsp, mrt_gas.tariff_gsp) as gsp,

             -- elec
             dcf.elec_reg_status,
             mrt_elec.tariff_id                                as elec_tariff_id,
             mrt_elec.ta_start                                 as elec_tariff_start,
             mrt_elec.ta_end                                   as elec_tariff_end,
             nvl(mrt_elec.unit_rate, -1)                       as elec_unit_rate,
             nvl(mrt_elec.standing_charge, -1)                 as elec_standing_charge,
             nvl(ensek_latest.elec_ur, -1)                     as ensek_elec_ur,
             nvl(ensek_latest.elec_sc, -1)                     as ensek_elec_sc,

             -- gas
             dcf.gas_reg_status,
             mrt_gas.tariff_id                                 as gas_tariff_id,
             mrt_gas.ta_start                                  as gas_tariff_start,
             mrt_gas.ta_end                                    as gas_tariff_end,
             nvl(mrt_gas.unit_rate, -1)                        as gas_unit_rate,
             nvl(mrt_gas.standing_charge, -1)                  as gas_standing_charge,
             nvl(ensek_latest.gas_ur, -1)                      as ensek_gas_ur,
             nvl(ensek_latest.gas_sc, -1)                      as ensek_gas_sc

      from ref_calculated_daily_customer_file dcf
               left join most_recent_tariffs mrt_elec
                         on dcf.account_id = mrt_elec.account_id and mrt_elec.fuel_type = 'E'
               left join most_recent_tariffs mrt_gas on dcf.account_id = mrt_gas.account_id and mrt_gas.fuel_type = 'G'
               left join vw_latest_rates ensek_latest on dcf.account_id = ensek_latest.account_id
      where elec_unit_rate != ensek_elec_ur
         or elec_standing_charge != ensek_elec_sc
         or gas_unit_rate != ensek_gas_ur
         or gas_standing_charge != ensek_gas_sc
      order by dcf.account_id) tariff_diffs
where account_status = 'Live'

select *
from "igloosense-uat".public.vw_latest_rates
where account_id = 54977


select rt_elec.gsp_ldz,
       rt_elec.name,
       rt_elec.billing_start_date,
       rt_elec.signup_start_date,
       rt_elec.end_date,
       rt_elec.id              as elec_id,
       rt_elec.standing_charge as elec_sc,
       rt_elec.unit_rate       as elec_ur,
       rt_gas.id               as gas_id,
       rt_gas.standing_charge  as gas_sc,
       rt_gas.unit_rate        as gas_ur
from (select * from ref_tariffs where fuel_type = 'E') rt_elec
         full join (select * from ref_tariffs where fuel_type = 'G') rt_gas
                   on rt_elec.gsp_ldz = rt_gas.gsp_ldz and rt_elec.billing_start_date = rt_gas.billing_start_date

select *
from "igloosense-uat".public.ref_calculated_tariff_accounts

-- DMRE-1009 repeatable check of current tariffs
with most_recent_tariffs as (
    select *
    from (
             select ta.account_id,
                    ta.start_date                                                                      as ta_start,
                    ta.end_date                                                                        as ta_end,
                    t.id                                                                               as tariff_id,
                    t.fuel_type,
                    t.gsp_ldz                                                                          as tariff_gsp,
                    t.unit_rate,
                    t.standing_charge,
                    row_number() over (partition by account_id, fuel_type order by ta.start_date desc) as rn
             from ref_calculated_tariff_accounts ta
                      left join ref_tariffs t on ta.tariff_id = t.id
         ) ordered_tariff_accounts
    where rn = 1
)
select dcf.account_id,
       dcf.account_status,
       ensek_lr.*
from ref_calculated_daily_customer_file dcf
         left join vw_latest_rates ensek_lr on dcf.account_id = ensek_lr.account_id
where account_status = 'Final'






































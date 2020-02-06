-- ########### ENSEK VIEW ###################
drop view if exists vw_latest_rates_ensek;
create view vw_latest_rates_ensek as
select most_recent_esc.account_id,
       most_recent_esc.start_date as elec_tariff_start,
       most_recent_esc.end_date   as elec_tariff_end,
       most_recent_esc.rate       as elec_sc,
       most_recent_eur.rate       as elec_ur,
       most_recent_gsc.start_date as gas_tariff_start,
       most_recent_gsc.end_date   as gas_tariff_end,
       most_recent_gsc.rate       as gas_sc,
       most_recent_gur.rate       as gas_ur
from (select *
      from (
               select *, row_number() over (partition by account_id order by start_date desc) rn
               from ref_tariff_history_elec_sc
           ) ordered_elec_sc
      where rn = 1) most_recent_esc
         full join (select *
                    from (
                             select *, row_number() over (partition by account_id order by start_date desc) rn
                             from ref_tariff_history_elec_ur
                         ) ordered_elec_ur
                    where rn = 1) most_recent_eur
                   on most_recent_esc.account_id = most_recent_eur.account_id
         full join (select *
                    from (
                             select *, row_number() over (partition by account_id order by start_date desc) rn
                             from ref_tariff_history_gas_sc
                         ) ordered_gas_sc
                    where rn = 1) most_recent_gsc
                   on most_recent_esc.account_id = most_recent_gsc.account_id
         full join (select *
                    from (
                             select *, row_number() over (partition by account_id order by start_date desc) rn
                             from ref_tariff_history_gas_ur
                         ) ordered_gas_ur
                    where rn = 1) most_recent_gur
                   on most_recent_esc.account_id = most_recent_gur.account_id
;

-- ################ IGLOO VIEW ########################
drop view if exists vw_latest_rates_igloo;
create or replace view vw_latest_rates_igloo as
    with most_recent_tariffs as (
        select ta.account_id,
               ta.start_date                                                                              as ta_start,
               ta.end_date                                                                                as ta_end,
               t.id                                                                                       as tariff_id,
               t.fuel_type,
               t.gsp_ldz                                                                                  as tariff_gsp,
               t.unit_rate,
               t.standing_charge,
               row_number()
               over (partition by account_id, fuel_type order by nvl(ta.end_date, getdate() + 1000) desc) as rn
        from ref_calculated_tariff_accounts ta
                 left join ref_tariffs t on ta.tariff_id = t.id
    )
    select nvl(mrt_elec.account_id, mrt_gas.account_id) as account_id,
           mrt_elec.tariff_id                           as elec_tariff_id,
           mrt_elec.ta_start                            as elec_tariff_start,
           mrt_elec.ta_end                              as elec_tariff_end,
           mrt_elec.standing_charge                     as elec_sc,
           mrt_elec.unit_rate                           as elec_ur,
           mrt_gas.tariff_id                            as gas_tariff_id,
           mrt_gas.ta_start                             as gas_tariff_start,
           mrt_gas.ta_end                               as gas_tariff_end,
           mrt_gas.standing_charge                      as gas_sc,
           mrt_gas.unit_rate                            as gas_ur
    from (select * from most_recent_tariffs where fuel_type = 'E' and rn = 1) mrt_elec
             full join (select * from most_recent_tariffs where fuel_type = 'G' and rn = 1) mrt_gas
                       on mrt_gas.account_id = mrt_elec.account_id
    order by account_id
;

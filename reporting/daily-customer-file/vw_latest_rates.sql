create view vw_latest_rates as
select most_recent_esc.account_id,
       most_recent_esc.rate as elec_sc,
       most_recent_eur.rate as elec_ur,
       most_recent_gsc.rate as gas_sc,
       most_recent_gur.rate as gas_ur
from (select account_id,
             start_date,
             end_date,
             rate
      from (
               select *, row_number() over (partition by account_id order by start_date desc) rn
               from ref_tariff_history_elec_sc
           ) ordered_elec_sc
      where rn = 1) most_recent_esc
         left join
     (select account_id,
             start_date,
             end_date,
             rate
      from (
               select *, row_number() over (partition by account_id order by start_date desc) rn
               from ref_tariff_history_elec_ur
           ) ordered_elec_ur
      where rn = 1) most_recent_eur
     on most_recent_esc.account_id = most_recent_eur.account_id
         left join
     (select account_id,
             start_date,
             end_date,
             rate
      from (
               select *, row_number() over (partition by account_id order by start_date desc) rn
               from ref_tariff_history_gas_sc
           ) ordered_gas_sc
      where rn = 1) most_recent_gsc
     on most_recent_esc.account_id = most_recent_gsc.account_id
         left join
     (select account_id,
             start_date,
             end_date,
             rate
      from (
               select *, row_number() over (partition by account_id order by start_date desc) rn
               from ref_tariff_history_gas_ur
           ) ordered_gas_ur
      where rn = 1) most_recent_gur
     on most_recent_esc.account_id = most_recent_gur.account_id
;

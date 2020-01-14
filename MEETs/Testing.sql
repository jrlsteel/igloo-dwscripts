-- 1
select cf.account_id
from ref_calculated_daily_customer_file cf
         left join ref_meets_eligibility me on cf.account_id = me.account_id
where me.account_id is null;

-- 2
select count(*)                       as num_records,
       max(num_s2_elec)               as most_elec_s2,
       min(num_s2_elec)               as least_elec_s2,
       count(num_s2_elec)             as num_elec_records,
       max(num_s2_gas)                as most_gas_s2,
       min(num_s2_gas)                as least_gas_s2,
       count(num_s2_gas)              as num_gas_records,
       sum(hh_consent::int)           as num_hh_true,
       sum((hh_consent = false)::int) as num_hh_false,
       count(hh_consent)              as num_hh_records,
       count(account_status)          as num_as_records
from ref_meets_eligibility;



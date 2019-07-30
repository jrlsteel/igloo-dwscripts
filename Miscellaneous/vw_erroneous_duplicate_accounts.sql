-- Find EDs by duplicated uuids in supply contracts
select *
from (SELECT sc.external_id                                                 as account_id,
             -- find the correct account_id for the external_uuid based on which has valid meterpoint info
             -- Where both duplicates have the same number of valid MPs, the one with fewest cancelled MPs is chosen
             first_value(external_id)
             over (partition by sc.external_uuid
                 order by rm.num_valid desc, rm.num_cancelled
                 rows between unbounded preceding and unbounded following ) as correct_account_id,
             sc.external_uuid,
             sc.created_at
      FROM ref_cdb_supply_contracts sc
               left join (select account_id,
                                 sum(case usage_flag when 'valid' then 1 else 0 end)     as num_valid,
                                 sum(case usage_flag when 'cancelled' then 1 else 0 end) as num_cancelled
                          from ref_meterpoints_raw
                          group by account_id) rm
                         on rm.account_id = sc.external_id
      WHERE sc.external_id is not null
     ) all_accs
where all_accs.account_id != all_accs.correct_account_id
order by correct_account_id, all_accs.account_id
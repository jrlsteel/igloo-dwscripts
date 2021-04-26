-- types are defined in SNOW code
-- 5002: SIGNUP_BONUS_TYPE_THREE_MONTH
-- 5003: SIGNUP_BONUS_TYPE_ONE_MONTH

with cte_account_info as (
    select sc.id                            as supply_contract_id,
           sc.external_id                   as account_id,
           dcf.acc_ssd                      as acc_ssd
    from ref_cdb_supply_contracts as sc
        inner join ref_calculated_daily_customer_file as dcf
            on sc.external_id = dcf.account_id
)
select
id                                                  as reward_transaction_id,
reward_account_id                                   as reward_account_id,
ref_id                                              as supply_contract_id,
account_info.account_id                             as account_id,
account_info.acc_ssd                                as account_ssd,
case
    when type = 5002 then 3
    when type = 5003 then 1
end                                                 as months,
amount,
created_at,
case
    when (account_ssd is not null)
        then add_months(account_ssd, months)
end                                                 as settles_at
from ref_cdb_reward_transactions as reward_transactions
    left join cte_account_info as account_info
        on reward_transactions.ref_id = account_info.supply_contract_id
where reward_transactions.type in (5002, 5003)
and reward_transactions.status = 1


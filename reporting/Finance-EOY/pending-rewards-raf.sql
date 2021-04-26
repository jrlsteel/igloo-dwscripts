-- types are defined in SNOW code
-- 1001: RAF_REFERRAL_TYPE
-- 1002: RAF_REFERRED_TYPE

with cte_reward_account_user as (
        select permissionable_id           as reward_account,
               user_id                     as user_id
        from ref_cdb_user_permissions up
        where permissionable_type = 'App\\RewardAccount'
        and permission_level = 0
    ),
    cte_users_supply_contract as (
        select max(permissionable_id)      as supply_contract,
               user_id                     as user_id
        from ref_cdb_user_permissions up
        where permissionable_type = 'App\\SupplyContract'
        and permission_level = 0
        group by user_id
    ),
    cte_account_status as (
        select sc.external_id              as account_id,
               sc.id                       as supply_contract_id,
               dcf.account_status          as account_status
        from ref_cdb_supply_contracts as sc
            inner join ref_calculated_daily_customer_file as dcf
                on sc.external_id = dcf.account_id
    )
select referrer.id                           as referrer_transaction_id,
       referred.id                           as referred_transaction_id,
       referrer.reward_account_id            as referrer_reward_account,
       referred.reward_account_id            as referred_reward_account,
       referrer_supply.supply_contract       as referrer_supply_contract,
       referals.referred_id                  as referred_supply_contract,
       referrer_account.account_id           as referrer_account_id,
       referred_account.account_id           as referred_account_id,
       referrer_account.account_status       as referrer_account_status,
       referred_account.account_status       as referred_account_status,
       referrer.amount                       as amount,
       referrer.created_at                   as created_date
from ref_cdb_raf_referrals as referals
    inner join ref_cdb_reward_transactions as referrer
        on referals.id = referrer.ref_id and
           referrer.type = 1001 and
           referrer.status = 1
    inner join ref_cdb_reward_transactions as referred
        on referals.id = referred.ref_id and
           referred.type = 1002 and
           referred.status = 1
    inner join cte_reward_account_user as referrer_user
        on referrer_user.reward_account = referrer.reward_account_id
    inner join cte_users_supply_contract as referrer_supply
        on referrer_user.user_id = referrer_supply.user_id
    inner join cte_account_status as referrer_account
        on referrer_supply.supply_contract = referrer_account.supply_contract_id
    left join cte_account_status as referred_account
        on referals.referred_id = referred_account.supply_contract_id









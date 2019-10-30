select distinct rm.account_id,
                users.email,
                func_sha1(lower(users.email)) as hash,
                case
                    when max(nvl(least(rm.supplyenddate, rm.associationenddate), getdate() + 1)) > getdate()
                        then 'current'
                    else 'past' end           as customer_type
from ref_meterpoints rm
         inner join ref_cdb_supply_contracts sc on rm.account_id = sc.external_id
         inner join ref_cdb_user_permissions up on up.permissionable_type = 'App\\SupplyContract' and
                                                   up.permissionable_id = sc.id
         inner join ref_cdb_users users on users.id = up.user_id
--where account_id in (1831, 54977)
group by rm.account_id, users.email
order by account_id


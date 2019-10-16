
select live_accounts.account_id,
       users.email,
       func_sha1(lower(users.email)) as hash
from (select distinct account_id
      from ref_meterpoints rm
      where ((rm.supplyenddate is null and rm.associationenddate is null)
          or least(supplyenddate, associationenddate) > getdate())) live_accounts
         inner join ref_cdb_supply_contracts sc on live_accounts.account_id = sc.external_id
         inner join ref_cdb_user_permissions up on up.permissionable_type = 'App\\SupplyContract' and
                                                   up.permissionable_id = sc.id
         inner join ref_cdb_users users on users.id = up.user_id
--where account_id in (1831, 54977)
order by account_id
-- create view vw_ensek_supply_contract_users as
select sc.id          as supply_contract_id,
       sc.external_id as ensek_id,
       up.user_id,
       sc.created_at as signup_date,
       acc_stat.ssd,
       acc_stat.sed,
       acc_stat.account_status

from ref_cdb_supply_contracts sc
         left join ref_cdb_user_permissions up
                   on sc.id = up.permissionable_id and
                      up.permissionable_type = 'App\\SupplyContract' and
                      up.permission_level = 0
         left join (select account_id,
                           min(greatest(associationstartdate, supplystartdate)) as ssd,
                           max(least(associationenddate, supplyenddate))        as sed,
                           udf_meterpoint_status(ssd, sed)                      as account_status
                    from ref_meterpoints mp
                    group by account_id) acc_stat
                   on acc_stat.account_id = sc.external_id
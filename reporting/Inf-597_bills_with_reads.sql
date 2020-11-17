-- link user id to ensek contract id
create or replace view vw_user_contracts as
select users.id                     as user_id,
       sc.external_id               as contract_id,
       'ensek_account'::varchar(32) as contract_type
from ref_cdb_users users
         left join ref_cdb_user_permissions up on up.user_id = users.id and
                                                  up.permission_level = 0 and
                                                  up.permissionable_type = 'App\\SupplyContract'
         left join ref_cdb_supply_contracts sc on up.permissionable_id = sc.id
where contract_id is not null
order by user_id, contract_type, contract_id;


-- ensek account start / end
create view vw_ensek_account_supply_status as
select account_id                                        as contract_id,
       min(mp_ssd)                                       as contract_ssd,
       nullif(max(mp_sed_not_null), '2100-01-01')        as contract_sed,
       udf_meterpoint_status(contract_ssd, contract_sed) as contract_status

from (select account_id,
             greatest(supplystartdate, associationstartdate) as mp_ssd,
             least(supplyenddate, associationenddate)        as mp_sed,
             nvl(mp_sed, '2100-01-01')                       as mp_sed_not_null
      from ref_meterpoints
      where mp_sed_not_null > mp_ssd) meterpoint_ssd_sed
group by account_id
order by account_id;


-- all intended bill dates for an ensek account
select ensek_account.contract_id,
       to_char(bill_dates.date, 'YYYY-MM') as bill_month,
       bill_dates.date

from vw_ensek_account_supply_status ensek_account
         left join aws_s3_stage2_extracts.stage2_accountsettings bill_settings
                   on ensek_account.contract_id = bill_settings.account_id
         left join ref_date bill_dates on bill_dates.day = bill_settings.billdayofmonth and
                                          bill_dates.date > ensek_account.contract_ssd and
                                          bill_dates.date <= nvl(ensek_account.contract_sed, getdate())
where contract_id = 54977


-- current GC
select gc_id_map.ensekid          as ensek_contract_id,
       count(mandates.mandate_id) as active_mandates,
       count(subscriptions.id)    as active_subscriptions,
       sum(subscriptions.amount)  as total_monthly_payment
from (select distinct client_id, ensekid
      from aws_fin_stage1_extracts.fin_go_cardless_api_clients) gc_id_map
         left join ref_fin_gocardless_mandates mandates
                   on mandates.customerid = gc_id_map.client_id and mandates.status = 'active'
         left join ref_fin_gocardless_subscriptions subscriptions
                   on subscriptions.mandate = mandates.mandate_id and
                      subscriptions.status = 'active'
group by ensek_contract_id

-- historic GC

select distinct resource_type, action
from aws_fin_stage1_extracts.fin_go_cardless_api_events
where resource_type in ('mandates', 'subscriptions')
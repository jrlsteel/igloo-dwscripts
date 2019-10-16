-- 1.1 find cases where an account is present in supply contracts or meterpoints but not in eligibility accounts or vice versa
select case when smea.account_id is null then 0 else 1 end as sme_filled,
       case when smea.account_id is null then 0 else 1 end as acc_listed
from ref_smart_meter_eligibility_accounts smea
         full join vw_supply_contracts_with_occ_accs acc_list on acc_list.external_id = smea.account_id
where sme_filled + acc_listed = 1

-- 1.2 find duplicates in smart eligibility accounts
select account_id, count(*) as cnt from ref_smart_meter_eligibility_accounts group by account_id having cnt > 1

-- 2 Check frequency of statuses
select eligibility_status, detailed_status, count(*) as cnt
from ref_smart_meter_eligibility_accounts
group by eligibility_status, detailed_status
order by detailed_status
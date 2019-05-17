/* Energy Switch Gaurantee - Refunds */
select
  ta.account_id as account_id,
  at2.creationdetail_createddate as transaction_date,
  at2.transactiontype as transaction_type,
  at2.transactiontypefriendlyname description,
  case when su.external_id is null then 'N' else 'Y' end account_id_found,
  case when at2.account_id is null then 'N' else 'Y' end last_transaction_with_refund_found
--   ac.status
from temp_account_id ta
     left outer join ref_cdb_supply_contracts su on su.external_id = ta.account_id
-- inner join ref_account_status ac on ac.account_id = su.external_id
left outer join (select at.* from aws_s3_stage2_extracts.stage2_accounttransactions at
                  inner join (select a.account_id,  max(creationdetail_createddate) created_date
                              from aws_s3_stage2_extracts.stage2_accounttransactions a
                              group by a.account_id) at1
                  on at.account_id = at1.account_id and at.creationdetail_createddate = at1.created_date
                where at.transactiontype = 'R') at2
         on at2.account_id = ta.account_id

order by ta.account_id;




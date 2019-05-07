--DMRE-434 Live customers as of 31st of march

/*
-- Get balances as of dd-mm-yyyy
select x.account_id, x.balance, x.* from (
select at.account_id,
       at.currentbalance as balance, at.creationdetail_createddate,
row_number() over (partition by account_id order by creationdetail_createddate desc) as row_number
 from aws_s3_stage2_extracts.stage2_accounttransactions at
  where creationdetail_createddate < '2019-03-31'
  ) x
 where x.row_number = 1
 and balance != 0
;
*/


-- Get accounts live as of dd-mm-yyyy
select
  sum(case when x.mp in('E', 'EG', 'GE') then 1 else 0 end) as accounts_atleast_1elec_mp,
  sum(case when x.mp in('G', 'EG', 'GE') then 1 else 0 end) as accounts_atleast_1gas_mp
  from (
select
account_id,
listagg(distinct meterpointtype) as mp
from ref_meterpoints
where supplystartdate <= ${date} and
(supplyenddate is null and
          (associationenddate is null or associationenddate > ${date}) or
           supplyenddate > ${date}
)
group by
account_id
) x;

--DMRE-434 Live customers as of 31st of march
select
  sum(case when x.mp in('E', 'EG', 'GE') then 1 else 0 end) as accounts_atleast_1elec_mp,
  sum(case when x.mp in('G', 'EG', 'GE') then 1 else 0 end) as accounts_atleast_1gas_mp
  from (
select
account_id,
listagg(distinct meterpointtype) as mp
from ref_meterpoints
where supplystartdate <= '2019-03-31' and
(supplyenddate is null and
          (associationenddate is null or associationenddate > '2019-03-31') or
           supplyenddate > '2019-03-31'
)
group by
account_id
) x;

-- Live+Lost customers as of 31st of march with balances != 0 (debit/credit)
select
  sum(case when x.mp in('E', 'EG', 'GE') and cast(x.balance as double precision) != 0 then 1 else 0 end) as accounts_atleast_1elec_mp_with_balances,
  sum(case when x.mp in('G', 'EG', 'GE') and cast(x.balance as double precision) != 0 then 1 else 0 end) as accounts_atleast_1gas_mp_with_balances
  from
  (
    select
    mp.account_id,
    listagg(distinct meterpointtype) as mp,
    max(s2.currentbalance) as balance
    from ref_meterpoints mp
    left outer join (select x.account_id, x.currentbalance from (
                          select at.account_id, at.currentbalance,
                          row_number() over (partition by account_id order by creationdetail_createddate desc) as row_number
                           from aws_s3_stage2_extracts.stage2_accounttransactions at
                           where creationdetail_createddate <= '2019-03-31') x
                     where x.row_number = 1) s2 on s2.account_id = mp.account_id
    where supplystartdate <= '2019-03-31'
group by
mp.account_id
) x;

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

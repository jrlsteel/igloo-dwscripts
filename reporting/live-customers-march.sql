-- Live customers as of 31st of march
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
 supplyenddate > '2019-03-31')
group by
account_id
) x;
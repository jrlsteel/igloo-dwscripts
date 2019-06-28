select account_id,
       count(supplyenddate) as seds,
       count(*) as counts,
       max(supplyenddate) as msed,
       current_date as cd
from ref_meterpoints rm
where greatest(rm.supplystartdate,rm.associationstartdate) <= least(rm.supplyenddate,rm.associationenddate)
group by rm.account_id
having count(supplyenddate)=count(*) and max(supplyenddate) < current_date
order by rm.account_id asc

select * from ref_meterpoints where account_id = 7723

select final_accounts.account_id,
       lastbill_date < final_accounts.end_date as final_billed,
       lastbill_date
from aws_s3_stage2_extracts.stage2_livebalances lb
    inner join (select account_id,
                       max(supplyenddate) as end_date
                from ref_meterpoints rm
                where greatest(rm.supplystartdate,rm.associationstartdate) <= least(rm.supplyenddate,rm.associationenddate)
                group by rm.account_id
                having count(supplyenddate)=count(*) and max(supplyenddate) < current_date) final_accounts
        on final_accounts.account_id = lb.account_id
where not final_billed
order by final_accounts.account_id

select * from aws_s3_stage2_extracts.stage2_livebalances where account_id = 1869
select max(supplyenddate) from ref_meterpoints where account_id = 1869
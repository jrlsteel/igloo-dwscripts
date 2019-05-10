--DMRE-451 Annual Statements Q1-2019
select
       x.meterpointtype,
       count(*) as T4_statements_due_to_be_sent,
       sum(case
             when x.createddate is not null
                    and x.createddate between period_start_date and period_end_date then 1
             else 0 end) as  T5_statemtents_issued_within_relevant_time,
       sum(case
             when x.createddate is not null
                    and x.createddate > period_end_date then 1
             else 0 end) as  T6_statemtents_issued_after_relevant_time,
       sum(case
             when x.createddate is null
                  then 1
             else 0 end) as  T6_statements_not_sent_yet
from
     (select
            su.external_id as account_id,
            mp.meterpointtype,
            mp.supplystartdate,
            mp.supplyenddate,
            ans.createddate,
            ans.energytype,
            to_date(${startdate}, 'YYYY-MM-DD') as period_start_date,
            to_date(${enddate}, 'YYYY-MM-DD') as period_end_date,
            case when supplystartdate is not null and
                      supplyenddate is not null and
                      supplystartdate > supplyenddate then 1 else 0 end as has_enddate_before_startdate
            from ref_cdb_supply_contracts su
                  left outer join ref_meterpoints mp on su.external_id = mp.account_id
                  left outer join temp_ref_annual_statements ans on ans.account_id = su.external_id and ans.energytype = mp.meterpointtype
            where
                  su.external_id is not null and
                  mp.supplystartdate is not null and
                  dateadd(months, 12, mp.supplystartdate) between ${startdate} and ${enddate} and
                  (
                   mp.supplyenddate is null
                    or
                  (mp.supplyenddate is not null and mp.supplyenddate >= dateadd(months, 12, mp.supplystartdate))
                  )
            ) x
where x.has_enddate_before_startdate = 0
group by x.meterpointtype;



-- Insert from stage2 to temp_ref as we dont have ETL running for annual statements
/*insert into temp_ref_annual_statements
    select cast (accountid as bigint),
    cast (amount as double precision),
    cast (createddate as timestamp),
    cast (energytype as varchar (10)),
    cast ("from" as timestamp),
    cast (statementid as bigint),
    cast ("to" as timestamp),
    cast (account_id as bigint)
    from
    aws_s3_stage2_extracts.stage2_annualstatements
    order by cast (accountid as bigint);
    ;

alter table temp_ref_annual_statements owner to igloo
;*/
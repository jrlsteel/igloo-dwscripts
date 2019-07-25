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
     (
     select
            su.external_id as account_id,
            mp.meterpointtype,
            mp.associationstartdate,
            mp.associationenddate,
            mp.supplystartdate,
            mp.supplyenddate,
            ans.createddate,
            ans.energytype,
            datediff(days, dateadd(months, 12, mp.associationstartdate), ans.createddate) as days_diff_asd,
            datediff(days, dateadd(months, 12, mp.supplystartdate), ans.createddate) as days_diff_ssd,
            case when mp.associationstartdate > mp.supplystartdate then 'Y' else '' end as home_move_account,
            to_date(${startdate}, 'YYYY-MM-DD') as period_start_date,
            to_date(${enddate}, 'YYYY-MM-DD') as period_end_date,
            case when supplystartdate is not null and
                      supplyenddate is not null and
                      supplystartdate > supplyenddate then 1 else 0 end as has_enddate_before_startdate
            from ref_cdb_supply_contracts su
                  left outer join ref_meterpoints_raw mp on su.external_id = mp.account_id
                  left outer join temp_ref_annual_statements ans on ans.account_id = su.external_id and ans.energytype = mp.meterpointtype
            where
                  su.external_id is not null and
                  mp.associationstartdate is not null and
                  (dateadd(months, 12, mp.associationstartdate + 5) between ${startdate} and ${enddate} or
                  dateadd(months, 12, mp.associationstartdate) between ${startdate} and ${enddate}) and
                  (
                   mp.associationenddate is null
                    or
                  (mp.associationenddate is not null and mp.associationenddate >= dateadd(months, 12, mp.associationenddate))
                  )
            ) x
where x.has_enddate_before_startdate = 0
group by x.meterpointtype;

select  x.meterpointtype,
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
from (
select * from (
 select
            su.external_id as account_id,
            mp.meterpointtype,
            mp.associationstartdate,
            mp.associationenddate,
            mp.supplystartdate,
            mp.supplyenddate,
            ans.createddate,
            ans.energytype,
            datediff(days, dateadd(months, 12, mp.associationstartdate), ans.createddate) as days_diff_asd,
            datediff(days, dateadd(months, 12, mp.supplystartdate), ans.createddate) as days_diff_ssd,
            case when mp.associationstartdate > mp.supplystartdate then 'Y' else '' end as home_move_account,
            case when mp.associationstartdate > mp.supplystartdate then mp.associationstartdate else mp.supplystartdate end as account_start_date,
            case when mp.associationstartdate > mp.supplystartdate then mp.associationenddate else mp.supplyenddate end as account_end_date,
            to_date(${startdate}, 'YYYY-MM-DD') as period_start_date,
            to_date(${enddate}, 'YYYY-MM-DD') as period_end_date,
            case when supplystartdate is not null and
                      supplyenddate is not null and
                      supplystartdate > supplyenddate then 1 else 0 end as has_enddate_before_startdate
            from ref_cdb_supply_contracts su
                  left outer join ref_meterpoints mp on su.external_id = mp.account_id
                  left outer join temp_ref_annual_statements ans on ans.account_id = su.external_id and ans.energytype = mp.meterpointtype
            ) x
            where
                  x.account_id is not null and
                  x.account_start_date is not null and
                  (dateadd(months, 12, x.account_start_date + 5) between ${startdate} and ${enddate} or
                  dateadd(months, 12, x.account_start_date) between ${startdate} and ${enddate} - 5) and
                   (
                    x.supplyenddate is null
                      or
                    (x.supplyenddate is not null and x.supplyenddate >= dateadd(months, 12, x.supplystartdate))
                   )
                   and
                   (
                     x.associationenddate is null
                      or
                     (x.associationenddate is not null and x.associationenddate >= dateadd(months, 12, x.associationstartdate))
                   )
and x.has_enddate_before_startdate = 0
order by x.account_id
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
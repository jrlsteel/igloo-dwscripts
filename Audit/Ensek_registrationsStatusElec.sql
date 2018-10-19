


/*
--PRECHECK QUERIES TO VALIDATE JOIN KEYS are UNIQUE
*/
select count(*) from aws_s3_ensec_api_extracts.cdb_stageregistrationselec ; --21628

select count(*) from (
select distinct account_id from aws_s3_ensec_api_extracts.cdb_stageregistrationselec)
; --21628


select account_id from aws_s3_ensec_api_extracts.cdb_stageregistrationselec
group by account_id
having count(*)>1
; --0

-- 0 duplicate rows

-- drop table ref_registrations_status_elec;
create table ref_registrations_status_elec
(
	account_id bigint encode delta,
	status varchar(255)
)
;

alter table ref_registrations_status_elec owner to igloo;

-- drop table ref_registrations_status_elec_audit;
create table ref_registrations_status_elec_audit
(
	account_id bigint encode delta,
	status varchar(255),
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_registrations_status_elec_audit owner to igloo;


-- New or Updated sql for audit table

-- insert into ref_registrations_status_elec_audit (
select cast(s.account_id as bigint) as account_id,
       trim(s.status) as status,
			 case when r.account_id is null then 'n' else 'u' end as etlchangetype,
			 current_timestamp as etchange
from aws_s3_ensec_api_extracts.cdb_stageregistrationselec s
left outer join ref_registrations_status_elec r on
        cast(s.account_id as bigint) = r.account_id
where trim(s.status) != trim(r.status)
			or r.account_id is null
-- )
;

--Insert to overwrite ref table
-- insert into ref_account_status (
select cast(s.account_id as bigint) as account_id,
       trim(s.status) as status
from aws_s3_ensec_api_extracts.cdb_stageregistrationselec s
-- )
;

-- TESTING --
--Run1:
--After 1st run
select count(*) from  aws_s3_ensec_api_extracts.cdb_stageregistrationselec s; --
select count(*) from ref_registrations_status_elec r; --
select r.etlchangetype,r.etlchange, count(*) from ref_registrations_status_elec_audit r group by r.etlchangetype, r.etlchange;

--update
update ref_account_status set status = 'Lost' where account_id = 10943;

--delete
delete from ref_account_status where account_id = 11437;

--Run2:
--Run the process again
-- Audit Checks
select r.etlchange, r.etlchangetype, count(1) from ref_registrations_status_elec_audit r group by r.etlchange, r.etlchangetype;
--1 new rows
--1 updated rows



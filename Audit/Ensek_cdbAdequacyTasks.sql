/*
--PRECHECK QUERIES TO VALIDATE JOIN KEYS are UNIQUE
*/
select count(*) from aws_s3_ensec_api_extracts.cdb_stagecdbadequacytasks ; --10947

select count(*) from (
select distinct id from aws_s3_ensec_api_extracts.cdb_stagecdbadequacytasks)
; --10947


select id from aws_s3_ensec_api_extracts.cdb_stagecdbadequacytasks
group by id
having count(*)>1
; --0

-- 0 duplicate rows

-- drop table ref_cdb_adequacy_tasks;
create table ref_cdb_adequacy_tasks
(
	id bigint,
	supply_contract_id bigint,
	user_id bigint,
	status varchar(255),
	opened_at timestamp,
	opened_user_id bigint,
	created_at timestamp,
	updated_at timestamp
)
;

alter table ref_cdb_adequacy_tasks owner to igloo;

-- drop table ref_cdb_adequacy_tasks_audit;
create table ref_cdb_adequacy_tasks_audit
(
	id bigint,
	supply_contract_id bigint,
	user_id bigint,
	status varchar(255),
	opened_at timestamp,
	opened_user_id bigint,
	created_at timestamp,
	updated_at timestamp,
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_cdb_adequacy_tasks_audit owner to igloo;


-- New or Updated sql for audit table

-- insert into ref_cdb_adequacy_tasks_audit (
select s.id,
			s.supply_contract_id,
			s.user_id,
			cast (s.status as varchar(255)) as status,
			s.opened_at,
			s.opened_user_id,
			s.created_at,
			s.updated_at,
		 case when r.id is null then 'n' else 'u' end as etlchangetype,
		 current_timestamp as etchange
from aws_s3_ensec_api_extracts.cdb_stagecdbadequacytasks s
left outer join ref_cdb_adequacy_tasks r on
        s.id = r.id
where s.supply_contract_id != r.supply_contract_id
	 		or s.user_id != r.user_id
	 		or trim(s.status) != trim(r.status)
	 		or s.opened_at != r.opened_at
	 		or trim(s.opened_user_id) != r.opened_user_id
	 		or s.created_at != r.created_at
	 		or s.updated_at != r.updated_at
			or r.id is null
-- )
;

--Insert to overwrite ref table
-- insert into ref_cdb_adequacy_tasks (
select s.id,
			s.supply_contract_id,
			s.user_id,
			cast (s.status as varchar(255)) as status,
			s.opened_at,
			s.opened_user_id,
			s.created_at,
			s.updated_at
from aws_s3_ensec_api_extracts.cdb_stagecdbadequacytasks s
-- )
;

-- TESTING --
--Run1:
--After 1st run
select count(*) from  aws_s3_ensec_api_extracts.cdb_stagecdbadequacytasks s; --
select count(*) from ref_cdb_adequacy_tasks r; --
select r.etlchangetype,r.etlchange, count(*) from ref_cdb_adequacy_tasks_audit r group by r.etlchangetype, r.etlchange;

--update
update ref_cdb_adequacy_tasks set status = 'exception' where id = 1242;

--delete
delete from ref_cdb_adequacy_tasks where id = 4045;

--Run2:
--Run the process again
-- Audit Checks
select r.etlchange, r.etlchangetype, count(1) from ref_cdb_adequacy_tasks_audit r group by r.etlchange, r.etlchangetype;
--1 new rows
--1 updated rows



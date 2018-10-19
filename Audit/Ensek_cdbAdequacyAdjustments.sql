


/*
--PRECHECK QUERIES TO VALIDATE JOIN KEYS are UNIQUE
*/
select count(*) from aws_s3_ensec_api_extracts.cdb_stagecdbadequacyadjustments ; --4410

select count(*) from (
select distinct id from aws_s3_ensec_api_extracts.cdb_stagecdbadequacyadjustments)
; --4410


select id from aws_s3_ensec_api_extracts.cdb_stagecdbadequacyadjustments
group by id
having count(*)>1
; --0

-- 0 duplicate rows

-- drop table ref_cdb_adequacy_adjustments;
create table ref_cdb_adequacy_adjustments
(
	id bigint,
	adequacy_task_id bigint,
	eacaq_reliability_data_id bigint,
	dd_coverage_data_id bigint,
	current_balance double precision,
	adequacy_needed boolean,
	debt_needed boolean,
	contact_needed boolean,
	active_management_task_id bigint,
	created_at timestamp,
	updated_at timestamp
)
;



alter table ref_cdb_adequacy_adjustments owner to igloo;

drop table ref_cdb_adequacy_adjustments_audit;
create table ref_cdb_adequacy_adjustments_audit
(
	id bigint,
	adequacy_task_id bigint,
	eacaq_reliability_data_id bigint,
	dd_coverage_data_id bigint,
	current_balance double precision,
	adequacy_needed boolean,
	debt_needed boolean,
	contact_needed boolean,
	active_management_task_id bigint,
	created_at timestamp,
	updated_at timestamp,
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_cdb_adequacy_adjustments_audit owner to igloo;


-- New or Updated sql for audit table

-- insert into ref_cdb_adequacy_adjustments_audit (
select s.id,
				s.adequacy_task_id,
				s.eacaq_reliability_data_id,
				s.dd_coverage_data_id,
				s.current_balance,
				s.adequacy_needed,
				s.debt_needed,
				s.contact_needed,
				s.active_management_task_id,
				s.created_at,
				s.updated_at,
		 case when r.id is null then 'n' else 'u' end as etlchangetype,
		 current_timestamp as etchange
from aws_s3_ensec_api_extracts.cdb_stagecdbadequacyadjustments s
left outer join ref_cdb_adequacy_adjustments r on
        s.id = r.id
where 	s.adequacy_task_id != r.adequacy_task_id
				or s.eacaq_reliability_data_id != r.eacaq_reliability_data_id
				or s.dd_coverage_data_id != r.dd_coverage_data_id
				or s.current_balance != r.current_balance
				or s.adequacy_needed != r.adequacy_needed
				or s.debt_needed != r.debt_needed
				or s.contact_needed != r.contact_needed
				or s.active_management_task_id != r.active_management_task_id
				or s.created_at != r.created_at
				or s.updated_at != r.updated_at
			or r.id is null
-- )
;

--Insert to overwrite ref table
-- insert into ref_cdb_adequacy_adjustments (
select s.id,
				s.adequacy_task_id,
				s.eacaq_reliability_data_id,
				s.dd_coverage_data_id,
				s.current_balance,
				s.adequacy_needed,
				s.debt_needed,
				s.contact_needed,
				s.active_management_task_id,
				s.created_at,
				s.updated_at
from aws_s3_ensec_api_extracts.cdb_stagecdbadequacyadjustments s
-- )
;

-- TESTING --
--Run1:
--After 1st run
select count(*) from  aws_s3_ensec_api_extracts.cdb_stagecdbadequacyadjustments s; --
select count(*) from ref_cdb_adequacy_adjustments r; --
select r.etlchangetype,r.etlchange, count(*) from ref_cdb_adequacy_adjustments_audit r group by r.etlchangetype, r.etlchange;

--update
update ref_cdb_adequacy_adjustments set  current_balance = 99.88 where id = 93;

--delete
delete from ref_cdb_adequacy_adjustments where id = 793;

--Run2:
--Run the process again
-- Audit Checks
select r.etlchange, r.etlchangetype, count(1) from ref_cdb_adequacy_adjustments_audit r group by r.etlchange, r.etlchangetype;
--1 new rows
--1 updated rows



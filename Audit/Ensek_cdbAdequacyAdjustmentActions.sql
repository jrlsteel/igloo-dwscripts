


/*
--PRECHECK QUERIES TO VALIDATE JOIN KEYS are UNIQUE
*/
select count(*) from aws_s3_ensec_api_extracts.cdb_stagecdbadequacyadjustmentactions ; --1466

select count(*) from (
select distinct id from aws_s3_ensec_api_extracts.cdb_stagecdbadequacyadjustmentactions)
; --1466


select id from aws_s3_ensec_api_extracts.cdb_stagecdbadequacyadjustmentactions
group by id
having count(*)>1
; --0

-- 0 duplicate rows

-- drop table ref_cdb_adequacy_adjustmentactions;
create table ref_cdb_adequacy_adjustmentactions
(
	id bigint,
	adequacy_adjustment_id bigint,
	adjusted_by_id bigint,
	adjustment_action varchar(255),
	payments_number int,
	payments_amount double precision,
	payments_created boolean,
	payments_refund boolean,
	dd_from double precision,
	dd_to double precision,
	next_payment_date timestamp,
	email_header varchar(500),
	next_run_date timestamp,
	created_at timestamp,
	updated_at timestamp
)
;

alter table ref_cdb_adequacy_adjustmentactions owner to igloo;

-- drop table ref_cdb_adequacy_adjustmentactions_audit;
create table ref_cdb_adequacy_adjustmentactions_audit
(
	id bigint,
	adequacy_adjustment_id bigint,
	adjusted_by_id bigint,
	adjustment_action varchar(255),
	payments_number int,
	payments_amount double precision,
	payments_created boolean,
	payments_refund boolean,
	dd_from double precision,
	dd_to double precision,
	next_payment_date timestamp,
	email_header varchar(500),
	next_run_date timestamp,
	created_at timestamp,
	updated_at timestamp,
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_cdb_adequacy_adjustmentactions_audit owner to igloo;


-- New or Updated sql for audit table

insert into ref_cdb_adequacy_adjustmentactions_audit (
select
			s.id,
			s.adequacy_adjustment_id,
			s.adjusted_by_id,
			cast (s.adjustment_action as varchar(255)),
			s.payments_number,
			cast (s.payments_amount as double precision),
			s.payments_created,
			s.payments_refund,
			cast (s.dd_from as double precision),
			cast (s.dd_to as double precision),
			s.next_payment_date,
			cast (s.email_header as varchar(500)),
			s.next_run_date,
			s.created_at,
			s.updated_at,
		 case when r.id is null then 'n' else 'u' end as etlchangetype,
		 current_timestamp as etchange
from aws_s3_ensec_api_extracts.cdb_stagecdbadequacyadjustmentactions s
left outer join ref_cdb_adequacy_adjustmentactions r on
        s.id = r.id
where
			s.id != r.id
			or s.adequacy_adjustment_id != r.adequacy_adjustment_id
			or s.adjusted_by_id != r.adjusted_by_id
			or trim(s.adjustment_action) != trim(r.adjustment_action)
			or s.payments_number != r.payments_number
			or cast (s.payments_amount as double precision) != r.payments_amount
			or s.payments_created != r.payments_created
			or s.payments_refund != r.payments_refund
			or cast (s.dd_from as double precision) != r.dd_from
			or cast (s.dd_to as double precision) != r.dd_to
			or s.next_payment_date != r.next_payment_date
			or trim(s.email_header) != trim(r.email_header)
			or s.next_run_date != r.next_run_date
			or s.created_at != r.created_at
			or s.updated_at != r.updated_at
			or r.id is null
)
;

--Insert to overwrite ref table
insert into ref_cdb_adequacy_adjustmentactions (
select
			s.id,
			s.adequacy_adjustment_id,
			s.adjusted_by_id,
			cast (s.adjustment_action as varchar(255)),
			s.payments_number,
			cast (s.payments_amount as double precision),
			s.payments_created,
			s.payments_refund,
			cast (s.dd_from as double precision),
			cast (s.dd_to as double precision),
			s.next_payment_date,
			cast (s.email_header as varchar(500)),
			s.next_run_date,
			s.created_at,
			s.updated_at
from aws_s3_ensec_api_extracts.cdb_stagecdbadequacyadjustmentactions s
)
;

-- TESTING --
--Run1:
--After 1st run
select count(*) from  aws_s3_ensec_api_extracts.cdb_stagecdbadequacyadjustmentactions s; --
select count(*) from ref_cdb_adequacy_adjustmentactions r; --
select r.etlchangetype,r.etlchange, count(*) from ref_cdb_adequacy_adjustmentactions_audit r group by r.etlchangetype, r.etlchange;

--update
update ref_cdb_adequacy_adjustmentactions set  payments_amount = 99.88 where id = 93;

--delete
delete from ref_cdb_adequacy_adjustmentactions where id = 793;

--Run2:
--Run the process again
-- Audit Checks
select r.etlchange, r.etlchangetype, count(1) from ref_cdb_adequacy_adjustmentactions_audit r group by r.etlchange, r.etlchangetype;
--1 new rows
--1 updated rows



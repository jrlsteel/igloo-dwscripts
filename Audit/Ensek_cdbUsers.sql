


/*
--PRECHECK QUERIES TO VALIDATE JOIN KEYS are UNIQUE
*/
select count(*) from aws_s3_ensec_api_extracts.cdb_stagecdbusers ; --22501

select count(*) from (
select distinct id from aws_s3_ensec_api_extracts.cdb_stagecdbusers)
; --22501


select id from aws_s3_ensec_api_extracts.cdb_stagecdbusers
group by id
having count(*)>1
; --0

-- 0 duplicate rows

drop table ref_cdb_users;
create table ref_cdb_users
(
	id bigint,
	registration_id bigint,
	ensek_id bigint,
	uuid varchar(255),
	zendesk_id bigint,
	title varchar(255),
	first_name varchar(255),
	last_name varchar(255),
	email varchar(255),
	phone_number varchar(255),
	mobile_number varchar(255),
	password varchar(255),
	date_of_birth timestamp,
	address varchar(1000),
	supply_address_id bigint,
	billing_addr varchar(1000),
	contact_address_id bigint,
	remember_token varchar(100),
	created_at timestamp,
	updated_at timestamp
)
;

alter table ref_cdb_users owner to igloo;

drop table ref_cdb_users_audit;
create table ref_cdb_users_audit
(
	id bigint,
	registration_id bigint,
	ensek_id bigint,
	uuid varchar(255),
	zendesk_id bigint,
	title varchar(255),
	first_name varchar(255),
	last_name varchar(255),
	email varchar(255),
	phone_number varchar(255),
	mobile_number varchar(255),
	password varchar(255),
	date_of_birth timestamp,
	address varchar(1000),
	supply_address_id bigint,
	billing_addr varchar(1000),
	contact_address_id bigint,
	remember_token varchar(100),
	created_at timestamp,
	updated_at timestamp,
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_cdb_users_audit owner to igloo;


-- New or Updated sql for audit table

-- insert into ref_cdb_users_audit (
-- select count(*) from (
select
		s.id,
		s.registration_id,
		s.ensek_id,
		trim(s.uuid),
		s.zendesk_id,
		trim(regexp_replace(trim(s.title), '\\t','')),
		trim(regexp_replace(trim(s.first_name), '\\t','')),
		trim(regexp_replace(trim(s.last_name), '\\t','')),
		trim(regexp_replace(trim(s.email), '\\t','')),
		trim(s.phone_number),
		trim(s.mobile_number),
		trim(s.password),
		cast(s.date_of_birth as timestamp),
		s.address,
		s.supply_address_id,
		trim(s.billing_addr),
		s.contact_address_id,
		trim(s.remember_token),
		s.created_at,
		s.updated_at,
		case when r.id is null then 'n' else 'u' end as etlchangetype,
	 	current_timestamp as etchange
from table_s3 s
left outer join table_rs r on
					s.id = r.id
where
		s.id != r.id
		or s.registration_id != r.registration_id
		or s.ensek_id != r.ensek_id
		or trim(s.uuid) != trim(r.uuid)
		or s.zendesk_id != r.zendesk_id
		or trim(regexp_replace(trim(s.title), '\\t','')) != trim(regexp_replace(trim(r.title), '\\t', ''))
		or trim(regexp_replace(trim(s.first_name), '\\t','')) != trim(regexp_replace(trim(r.first_name), '\\t', ''))
		or trim(regexp_replace(trim(s.last_name), '\\t','')) != trim(regexp_replace(trim(r.last_name), '\\t', ''))
		or trim(regexp_replace(trim(s.email), '\\t','')) != trim(regexp_replace(trim(r.email), '\\t', ''))
		or trim(s.phone_number) != trim(r.phone_number)
		or trim(s.mobile_number) != trim(r.mobile_number)
		or trim(s.password) != trim(r.password)
		or cast(s.date_of_birth as timestamp) != r.date_of_birth
		or s.address != r.address
		or s.supply_address_id != r.supply_address_id
		or trim(s.billing_addr) != trim(r.billing_addr)
		or s.contact_address_id != r.contact_address_id
		or trim(s.remember_token) != trim(r.remember_token)
		or s.created_at != r.created_at
		or s.updated_at != r.updated_at
		or r.id is null
-- )
-- )
;

--Insert to overwrite ref table
-- insert into ref_cdb_users (
select
			s.id,
		s.registration_id,
		s.ensek_id,
		trim(s.uuid),
		s.zendesk_id,
		trim(regexp_replace(trim(s.title), '\\t','')),
		trim(regexp_replace(trim(s.first_name), '\\t','')),
		trim(regexp_replace(trim(s.last_name), '\\t','')),
		trim(regexp_replace(trim(s.email), '\\t','')),
		trim(s.phone_number),
		trim(s.mobile_number),
		trim(s.password),
		cast(s.date_of_birth as timestamp),
		s.address,
		s.supply_address_id,
		trim(s.billing_addr),
		s.contact_address_id,
		trim(s.remember_token),
		s.created_at,
		s.updated_at
from aws_s3_ensec_api_extracts.cdb_stagecdbusers s
-- )
;

-- TESTING --
--Run1:
--After 1st run
select count(*) from  aws_s3_ensec_api_extracts.cdb_stagecdbusers s; --
select count(*) from ref_cdb_users r; --
select r.etlchangetype,r.etlchange, count(*) from ref_cdb_users_audit r group by r.etlchangetype, r.etlchange;

--update
update ref_cdb_users set  email = 'testemail@igloo.energy' where id = 12290;



--delete
delete from ref_cdb_users where id = 3163;

--Run2:
--Run the process again
-- Audit Checks
select r.etlchange, r.etlchangetype, count(1) from ref_cdb_users_audit r group by r.etlchange, r.etlchangetype;
--1 new rows
--1 updated rows

-- CHECK to get unique keys
select count(*) from ref_cdb_supply_contracts; --21890
select count(*) from (
select distinct r.id from ref_cdb_supply_contracts r) --21890;


create table ref_cdb_supply_contracts
(
	id bigint encode delta,
	supply_address_id bigint encode delta,
	registration_id bigint encode delta,
	external_id bigint,
	external_uuid varchar(255),
	status varchar(255),
	created_at timestamp,
	updated_at timestamp
)
;

alter table ref_cdb_supply_contracts owner to igloo
;

create table ref_cdb_supply_contracts_audit
(
	id bigint encode delta,
	supply_address_id bigint encode delta,
	registration_id bigint encode delta,
	external_id bigint,
	external_uuid varchar(255),
	status varchar(255),
	created_at timestamp,
	updated_at timestamp,
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_cdb_supply_contracts_audit owner to igloo
;

-- New or Updated sql for sudit table
-- insert into ref_cdb_supply_contracts_audit (
select 	cast (s.id as bigint) as id,
	cast (s.supply_address_id as bigint) as supply_address_id,
	cast (s.registration_id as bigint) as registration_id,
	cast (s.external_id as bigint) as external_id,
	trim (s.external_uuid) as external_uuid,
	trim (s.status) as status,
	cast (s.created_at as timestamp) as created_at,
	cast (s.updated_at as timestamp) as updated_at,
  case when r.id is null then 'n' else 'u' end as etlchangetype,
  current_timestamp as etlchange
from aws_s3_ensec_api_extracts.cdb_stagecdbsupplycontract s
left outer join ref_cdb_supply_contracts r ON
      s.id = r.id
where cast (s.supply_address_id as bigint) != r.supply_address_id
      or cast (s.registration_id as bigint) != r.registration_id
      or cast (s.external_id as bigint) != r.external_id
      or trim (s.external_uuid) != trim(r.external_uuid)
      or trim(s.status) != trim (r.status)
      or cast (s.created_at as timestamp) != r.created_at
      or cast (s.updated_at as timestamp) != r.updated_at
      or r.id is null
--     )
;

-- New for Overwrite
-- insert into ref_cdb_supply_contracts (
select cast (s.id as bigint) as id,
	cast (s.supply_address_id as bigint) as supply_address_id,
	cast (s.registration_id as bigint) as registration_id,
	cast (s.external_id as bigint) as external_id,
	trim (s.external_uuid),
	trim (s.status),
	cast (s.created_at as timestamp) as created_at,
	cast (s.updated_at as timestamp) as updated_at
from aws_s3_ensec_api_extracts.cdb_stagecdbsupplycontract s
--     )
;

--Testing
-- Run1:
-- After 1st Run
select count(*) from ref_cdb_supply_contracts r;
select count(*) from  aws_s3_ensec_api_extracts.cdb_stagecdbsupplycontract s; --21890
select count(*) from  ref_cdb_supply_contracts_audit; --21890


select r.etlchangetype,r.etlchange, count(*) from ref_cdb_supply_contracts_audit r group by r.etlchangetype, r.etlchange;


--update
update ref_cdb_supply_contracts set registration_id = 99999 where id = 19269;
--1 rows

--delete
delete from ref_cdb_supply_contracts where id = 10699;
--1 rows

--Run 2:
--Run the process again
-- Audit Checks
select r.etlchange, r.etlchangetype, count(1) from ref_cdb_supply_contracts_audit r group by r.etlchange, r.etlchangetype;
--1 new row
--1 updated row
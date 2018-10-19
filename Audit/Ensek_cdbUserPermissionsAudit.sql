-- CHECK to get unique keys
select count(*) from ref_cdb_user_permissions; --21890
select count(*) from (
select distinct r.id from ref_cdb_user_permissions r) --21890;


create table ref_cdb_user_permissions
(
	id bigint encode delta,
	user_id bigint encode delta,
	permissionable_id bigint encode delta,
	permissionable_type varchar(255),
	permission_level bigint,
	created_at timestamp,
	updated_at timestamp
)
;

alter table ref_cdb_user_permissions owner to igloo
;

create table ref_cdb_user_permissions_audit
(
	id bigint encode delta,
	user_id bigint encode delta,
	permissionable_id bigint encode delta,
	permissionable_type varchar(255),
	permission_level bigint,
	created_at timestamp,
	updated_at timestamp,
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_cdb_user_permissions_audit owner to igloo
;

select count(*) from ref_cdb_user_permissions r;
select count(*) from  aws_s3_ensec_api_extracts.cdb_stagecdbuserpermissions s; --21890

-- New or Updated sql for audit table
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

--TESTING
--Run1:
--After 1st run
select count(*) from  aws_s3_ensec_api_extracts.cdb_stagecdbuserpermissions s; --21890
select count(*) from ref_cdb_user_permissions r; --21890
select r.etlchangetype,r.etlchange, count(*) from ref_cdb_user_permissions_audit r group by r.etlchangetype, r.etlchange;


--update
update ref_cdb_user_permissions set permissionable_id = 99999 where id = 20905;

--delete
delete from ref_cdb_user_permissions where id = 17823;

--Run2:
--Run the process again
-- Audit Checks
select r.etlchange, r.etlchangetype, count(1) from ref_cdb_user_permissions_audit r group by r.etlchange, r.etlchangetype;
--1 new rows
--1 updated rows

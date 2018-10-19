


/*
--PRECHECK QUERIES TO VALIDATE JOIN KEYS are UNIQUE
*/
select count(*) from aws_s3_ensec_api_extracts.cdb_stagecdbaddresses ; --24776

select count(*) from (
select distinct id from aws_s3_ensec_api_extracts.cdb_stagecdbaddresses)
; --24776


select id from aws_s3_ensec_api_extracts.cdb_stagecdbaddresses
group by id
having count(*)>1
; --0

-- 0 duplicate rows

drop table ref_cdb_addresses;
create table ref_cdb_addresses
(
	id bigint,
	sub_building_name_number varchar(255),
	building_name_number varchar(255),
	dependent_thoroughfare varchar(255),
	thoroughfare varchar(255),
	double_dependent_locality varchar(255),
	dependent_locality varchar(255),
	post_town varchar(255),
	county varchar(255),
	postcode varchar(255),
	uprn varchar(255),
	created_at timestamp,
	updated_at timestamp
)
;

alter table ref_cdb_addresses owner to igloo;

drop table ref_cdb_addresses_audit;
create table ref_cdb_addresses_audit
(
	id bigint,
	sub_building_name_number varchar(255),
	building_name_number varchar(255),
	dependent_thoroughfare varchar(255),
	thoroughfare varchar(255),
	double_dependent_locality varchar(255),
	dependent_locality varchar(255),
	post_town varchar(255),
	county varchar(255),
	postcode varchar(255),
	uprn varchar(255),
	created_at timestamp,
	updated_at timestamp,
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_cdb_addresses_audit owner to igloo;


-- New or Updated sql for audit table

-- insert into ref_cdb_addresses_audit (
-- select count(*) from (
select
		s.id,
		trim(s.sub_building_name_number),
		trim(s.building_name_number),
		trim(s.dependent_thoroughfare),
		trim(s.thoroughfare),
		trim(s.double_dependent_locality),
		trim(s.dependent_locality),
		trim(s.post_town),
		trim(s.county),
		trim(s.postcode),
		trim(s.uprn),
		s.created_at,
		s.updated_at,
		case when r.id is null then 'n' else 'u' end as etlchangetype,
	 	current_timestamp as etchange
from aws_s3_ensec_api_extracts.cdb_stagecdbaddresses s
left outer join ref_cdb_addresses r on
					s.id = r.id
where
		trim(s.sub_building_name_number) != r.sub_building_name_number
		or trim(s.building_name_number) != r.building_name_number
		or trim(s.dependent_thoroughfare) != r.dependent_thoroughfare
		or trim(s.thoroughfare) != r.thoroughfare
		or trim(s.double_dependent_locality) != r.double_dependent_locality
		or trim(s.dependent_locality) != r.dependent_locality
		or trim(s.post_town) != r.post_town
		or trim(s.county) != r.county
		or trim(s.postcode) != r.postcode
		or trim(s.uprn) != r.uprn
		or s.created_at != r.created_at
		or s.updated_at != r.updated_at
		or r.id is null
-- )
-- )
;

--Insert to overwrite ref table
-- insert into ref_cdb_addresses (
select
		s.id,
		trim(s.sub_building_name_number),
		trim(s.building_name_number),
		trim(s.dependent_thoroughfare),
		trim(s.thoroughfare),
		trim(s.double_dependent_locality),
		trim(s.dependent_locality),
		trim(s.post_town),
		trim(s.county),
		trim(s.postcode),
		trim(s.uprn),
		s.created_at,
		s.updated_at
from aws_s3_ensec_api_extracts.cdb_stagecdbaddresses s
-- )
;

-- TESTING --
--Run1:
--After 1st run
select count(*) from  aws_s3_ensec_api_extracts.cdb_stagecdbaddresses s; --24776
select count(*) from ref_cdb_addresses r; --24776
select r.etlchangetype,r.etlchange, count(*) from ref_cdb_addresses_audit r group by r.etlchangetype, r.etlchange;

--update
update ref_cdb_addresses set  thoroughfare = 'hill lane' where id = 13752;



--delete
delete from ref_cdb_addresses where id = 2387;

--Run2:
--Run the process again
-- Audit Checks
select r.etlchange, r.etlchangetype, count(1) from ref_cdb_addresses_audit r group by r.etlchange, r.etlchangetype;
--1 new rows
--1 updated rows

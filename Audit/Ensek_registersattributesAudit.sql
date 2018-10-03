/*
-- PRECHECK QUERIES to get the unique keys
select count(*) from ref_registers_attributes;--124378

--Get distinct count on unique keys
select count(*) from (
select distinct account_id,meter_point_id,meter_id,register_id,registersattributes_attributename,registersattributes_attributedescription from ref_registers_attributes
);--124358

select account_id,meter_point_id,meter_id,register_id,registersattributes_attributename,registersattributes_attributedescription from ref_registers_attributes
group by account_id,meter_point_id,meter_id,register_id,registersattributes_attributename,registersattributes_attributedescription
having count(1)>1;
--20 duplicate rows

 */



create table ref_registers_attributes
(
	account_id bigint,
	meter_point_id bigint,
	meter_id bigint,
	register_id bigint,
	registersattributes_attributename varchar(255),
	registersattributes_attributedescription varchar(255),
	registersattributes_attributevalue varchar(255)
)
;

alter table ref_registers_attributes owner to igloo
;

create table ref_registers_attributes_audit
(
	account_id bigint,
	meter_point_id bigint,
	meter_id bigint,
	register_id bigint,
	registersattributes_attributename varchar(255),
	registersattributes_attributedescription varchar(255),
	registersattributes_attributevalue varchar(255),
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_registers_attributes_audit owner to igloo
;


--New or Updated Registers SQL for audit table
-- insert into ref_registers_attributes_audit
select
		s.account_id,
		s.meter_point_id,
		s.meter_id,
		s.register_id,
		s.registersattributes_attributename,
		s.registersattributes_attributedescription,
		s.registersattributes_attributevalue,
		case when r.register_id is null then 'n' else 'u' end as etlchangetype,
		current_timestamp etlchange
from aws_s3_ensec_api_extracts.cdb_registersattributes s
       left outer join ref_registers_attributes r
       		ON s.account_id = r.account_id
       		and s.meter_point_id = r.meter_point_id
       		and s.meter_id = r.meter_id
       		and s.register_id = r.register_id
       		and s.registersattributes_attributename = r.registersattributes_attributename
       		and s.registersattributes_attributedescription = r.registersattributes_attributedescription
       	where s.registersattributes_attributevalue != r.registersattributes_attributevalue
					or r.register_id is null;


select
		s.account_id as account_id,
		s.meter_point_id as meter_point_id,
		s.meter_id as meter_id,
		s.register_id as register_id,
		cast (s.registersattributes_attributename as varchar(255)) as registersattributes_attributename,
		cast (s.registersattributes_attributedescription as varchar(255)) as registersattributes_attributedescription,
		cast (s.registersattributes_attributevalue as varchar(255)) as registersattributes_attributevalue
from aws_s3_ensec_api_extracts.cdb_registersattributes s
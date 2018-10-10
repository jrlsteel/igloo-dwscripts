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

-- insert into ref_registers_attributes_audit (
select
		cast (s.account_id as bigint) as account_id,
		cast (s.meter_point_id as bigint) as meter_point_id,
		cast (s.meter_id as bigint) as meter_id,
		cast (s.register_id as bigint) as register_id,
		trim(s.registersattributes_attributename) as registersattributes_attributename,
		trim(s.registersattributes_attributedescription) as registersattributes_attributedescription,
		trim(s.registersattributes_attributevalue) as registersattributes_attributevalue,
		case when r.register_id is null then 'n' else 'u' end as etlchangetype,
		current_timestamp as etlchange
from aws_s3_ensec_api_extracts.cdb_stageregistersattributes s
       left outer join ref_registers_attributes r
       		ON cast (s.account_id as bigint) = r.account_id
       		and cast (s.meter_point_id as bigint) = r.meter_point_id
       		and cast (s.meter_id as bigint) = r.meter_id
       		and cast (s.register_id as bigint) = r.register_id
       		and trim (s.registersattributes_attributename) = trim (r.registersattributes_attributename)
       		and trim (s.registersattributes_attributedescription) = trim (r.registersattributes_attributedescription)
       	where trim (s.registersattributes_attributevalue) != trim (r.registersattributes_attributevalue)
					or r.register_id is null
-- 		)
;


-- insert into ref_registers_attributes (
select
		cast (s.account_id as bigint) as account_id,
		cast (s.meter_point_id as bigint) as meter_point_id,
		cast (s.meter_id as bigint) as meter_id,
		cast (s.register_id as bigint) as register_id,
		trim(s.registersattributes_attributename) as registersattributes_attributename,
		trim(s.registersattributes_attributedescription) as registersattributes_attributedescription,
		trim(s.registersattributes_attributevalue) as registersattributes_attributevalue
from aws_s3_ensec_api_extracts.cdb_stageregistersattributes s
-- 		)
;

select count(*) from aws_s3_ensec_api_extracts.cdb_stageregistersattributes; --131986
select count(*) from ref_registers_attributes; --0

select r.etlchange, r.etlchangetype, count(*) from ref_registers_attributes_audit r group by r.etlchange, r.etlchangetype;
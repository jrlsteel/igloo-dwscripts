select count(*) from ref_registers_attributes;--2964

--Get distinct count on unique keys
select count(*) from (
select distinct account_id,meter_point_id,meter_id,register_id,registersattributes_attributename,registersattributes_attributedescription from ref_registers_attributes
--group by account_id,meter_point_id,meter_id,register_id,registersattributes_attributename,registersattributes_attributedescription
);
drop table ref_registers_attributes_audit;
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



--Updated Registers SQL
insert into ref_registers_attributes_audit
(
select
		s.account_id,
		s.meter_point_id,
		s.meter_id,
		s.register_id,
		s.registersattributes_attributename,
		s.registersattributes_attributedescription,
		s.registersattributes_attributevalue,
		'u', current_timestamp
from aws_s3_ensec_api_extracts.cdb_registersattributes s
       inner join ref_registers_attributes2 r
       		ON r.account_id = s.account_id
       		and r.meter_point_id = s.meter_point_id
       		and s.meter_id = r.meter_id
       		and r.register_id = s.register_id
       		and r.registersattributes_attributename = s.registersattributes_attributename
       		and r.registersattributes_attributedescription = s.registersattributes_attributedescription
       	where s.registersattributes_attributevalue != r.registersattributes_attributevalue
					);

--New Registers SQL
insert into ref_registers_attributes_audit
select
		s.account_id,
		s.meter_point_id,
		s.meter_id,
		s.register_id,
		s.registersattributes_attributename,
		s.registersattributes_attributedescription,
		s.registersattributes_attributevalue,
		'n', current_timestamp
from aws_s3_ensec_api_extracts.cdb_registersattributes s
       left outer join ref_registers_attributes2 r
       		ON r.account_id = s.account_id
       		and r.meter_point_id = s.meter_point_id
       		and s.meter_id = r.meter_id
       		and r.register_id = s.register_id
       		and r.registersattributes_attributename = s.registersattributes_attributename
       		and r.registersattributes_attributedescription = s.registersattributes_attributedescription
where r.register_id is null;


--Deleted Registers SQL
insert into ref_registers_attributes_audit
select
		r.account_id,
		r.meter_point_id,
		r.meter_id,
		r.register_id,
		r.registersattributes_attributename,
		r.registersattributes_attributedescription,
		r.registersattributes_attributevalue,
		'd', current_timestamp
from ref_registers_attributes2 r
       left outer join aws_s3_ensec_api_extracts.cdb_registersattributes s
       		ON r.account_id = s.account_id
       		and r.meter_point_id = s.meter_point_id
       		and s.meter_id = r.meter_id
       		and r.register_id = s.register_id
       		and r.registersattributes_attributename = s.registersattributes_attributename
       		and r.registersattributes_attributedescription = s.registersattributes_attributedescription
where s.register_id is null;

--Updated Registers SQL
insert into ref_registersattributes_audit
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
       inner join ref_registersattributes r
       		ON s.meter_id = r.meter_id
where (s.register_id != r.register_id
				 or s.registersattributes_attributename != r.registersattributes_attributename
				 or s.registersattributes_attributedescription != r.registersattributes_attributedescription
				 or s.registersattributes_attributevalue != r.registersattributes_attributevalue
					);


--New Registers SQL
insert into ref_registersattributes_audit
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
       left outer join ref_registersattributes r
       		ON s.meter_id = r.meter_id
where r.meter_id is null;


--Deleted Registers SQL
insert into ref_registersattributes_audit
select
		r.account_id,
		r.meter_point_id,
		r.meter_id,
		r.register_id,
		r.registersattributes_attributename,
		r.registersattributes_attributedescription,
		r.registersattributes_attributevalue,
		'u', current_timestamp
from ref_registersattributes r
       left outer join aws_s3_ensec_api_extracts.cdb_registersattributes s
       		ON r.meter_id = s.meter_id
where s.meter_id is null;

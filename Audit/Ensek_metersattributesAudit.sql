-- Updated MetersAttributes SQL
insert into ref_meters_audit
select
	s.account_id,
	s.meter_point_id,
	s.meter_id,
	s.metersattributes_attributename,
	s.metersattributes_attributedescription,
	s.metersattributes_attributevalue,
	'u', current_timestamp
from aws_s3_ensec_api_extracts.cdb_metersattributes s
inner join ref_metersattributes r
      ON s.meter_id = r.meter_id
where (s.metersattributes_attributename != r.metersattributes_attributename
			or s.metersattributes_attributedescription != r.metersattributes_attributedescription
			or s.metersattributes_attributevalue != r.metersattributes_attributevalue
			);

-- New MetersAttributes SQL
insert into ref_meters_audit
select
	s.account_id,
	s.meter_point_id,
	s.meter_id,
	s.metersattributes_attributename,
	s.metersattributes_attributedescription,
	s.metersattributes_attributevalue,
	'n', current_timestamp
from aws_s3_ensec_api_extracts.cdb_metersattributes s
left outer join ref_metersattributes r
		ON r.meter_id = s.meter_id
where r.meter_id is null;

-- Deleted MetersAttributes SQL
insert into ref_meters_audit
select
	r.account_id,
	r.meter_point_id,
	r.meter_id,
	r.metersattributes_attributename,
	r.metersattributes_attributedescription,
	r.metersattributes_attributevalue,
	'd', current_timestamp
from ref_metersattributes r
left outer join aws_s3_ensec_api_extracts.cdb_metersattributes s
		ON s.meter_id = r.meter_id
where s.meter_id is null;
-- Updated MeterPoints_Attributes SQL
insert into ref_meterpoints_attributes_audit
select
	s.account_id,
	s.meter_point_id,
	s.attributes_attributename,
	s.attributes_attributedescription,
	s.attributes_attributevalue,
	s.attributes_effectivefromdate,
	s.attributes_effectivetodate,
	'u', current_timestamp
from aws_s3_ensec_api_extracts.cdb_attributes s
inner join ref_meterpoints_attributes r
      ON s.meter_point_id = r.meter_point_id
where (s.attributes_attributename != r.attributes_attributename
			or s.attributes_attributedescription != r.attributes_attributedescription
			or s.attributes_attributevalue != r.attributes_attributevalue
			);

-- New MeterPoints_Attributes SQL
insert into ref_meterpoints_attributes_audit
select
	s.account_id,
	s.meter_point_id,
	s.attributes_attributename,
	s.attributes_attributedescription,
	s.attributes_attributevalue,
	s.attributes_effectivefromdate::timestamp,
	s.attributes_effectivetodate::timestamp,
	'n', current_timestamp
from aws_s3_ensec_api_extracts.cdb_attributes s
left outer join ref_meterpoints_attributes r
      ON r.meter_point_id = s.meter_point_id
where r.meter_point_id is null;


-- Deleted MetersAttributes SQL
insert into ref_meterpoints_attributes_audit
select
	r.account_id,
	r.meter_point_id,
	r.attributes_attributename,
	r.attributes_attributedescription,
	r.attributes_attributevalue,
	r.attributes_effectivefromdate,
	r.attributes_effectivetodate,
	'd', current_timestamp
from ref_meterpoints_attributes r
left outer join aws_s3_ensec_api_extracts.cdb_attributes s
      ON s.meter_point_id = r.meter_point_id
where s.meter_point_id is null;

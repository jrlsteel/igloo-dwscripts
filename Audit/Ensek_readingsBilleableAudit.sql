--Updated Readings SQL
insert into ref_readings_billeable_audit
select
		s.account_id,
		s.meter_point_id,
		s.id bigint,
		nullif(s.datetime,'')::timestamp,
		nullif(s.createddate,'')::timestamp,
		s.meterreadingsource,
		s.reading_id,
		s.reading_registerid,
		s.readingtype,
		s.reading_value,
		s.meterpointid,
		'u', current_timestamp
from aws_s3_ensec_api_extracts.cdb_readingsbilleable s
       inner join ref_readings_billeable r
       		ON s.id = r.id
where (nullif(s.datetime,'')::timestamp != r.datetime
		or nullif(s.createddate,'')::timestamp != r.createddate
		or s.meterreadingsource != r.meterreadingsource
		or s.reading_id != r.reading_id
		or s.reading_registerid != r.reading_registerid
		or s.readingtype != r.readingtype
		or s.reading_value != r.reading_value
		or s.meterpointid != r.meterpointid);


--New Readings SQL
insert into ref_readings_billeable_audit
select
		s.account_id,
		s.meter_point_id,
		s.id bigint,
		nullif(s.datetime,'')::timestamp,
		nullif(s.createddate,'')::timestamp,
		s.meterreadingsource,
		s.reading_id,
		s.reading_registerid,
		s.readingtype,
		s.reading_value,
		s.meterpointid,
		'n', current_timestamp
from aws_s3_ensec_api_extracts.cdb_readingsbilleable s
left outer join ref_readings_billeable r
      ON r.id = s.id
where r.id is null;

--Deleted Readings SQL
insert into ref_readings_billeable_audit
select
		r.account_id,
		r.meter_point_id,
		r.id bigint,
		nullif(r.datetime,'')::timestamp,
		nullif(r.createddate,'')::timestamp,
		r.meterreadingsource,
		r.reading_id,
		r.reading_registerid,
		r.readingtype,
		r.reading_value,
		r.meterpointid,
		'd', current_timestamp
from ref_readings_billeable r
left outer join aws_s3_ensec_api_extracts.cdb_readingsbilleable s
      ON s.id = r.id
where s.id is null;
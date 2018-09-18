-- Updated Meters SQL
insert into ref_meters_audit
select
	s.account_id,
	s.meter_point_id,
	s.meterid,
	s.meterserialnumber,
	nullif(s.installeddate,'')::timestamp,
	nullif(s.removeddate,'')::timestamp,
			 'u', current_timestamp
from aws_s3_ensec_api_extracts.cdb_meters s
inner join ref_meters r
      ON s.meterid = r.meter_id
where (s.meterserialnumber != r.meterserialnumber
			or nullif(s.installeddate, '')::timestamp != r.installeddate
			or nullif(s.removeddate, '')::timestamp != r.removeddate
			);


-- New Meters SQL
insert into ref_meters_audit
select
	s.account_id,
	s.meter_point_id,
	s.meterid,
	s.meterserialnumber,
	nullif(s.installeddate,'')::timestamp,
	nullif(s.removeddate,'')::timestamp,
			 'n', current_timestamp
from ref_meters r
left outer join aws_s3_ensec_api_extracts.cdb_meters s
      ON r.meterid = s.meter_id
where r.meterid is null;


-- Deleted Meters SQL
insert into ref_meters_audit
select
	r.account_id,
	r.meter_point_id,
	r.meter_id,
	r.meterserialnumber,
	nullif(r.installeddate,'')::timestamp,
	nullif(r.removeddate,'')::timestamp,
	'd', current_timestamp
from ref_meters r
left outer join aws_s3_ensec_api_extracts.cdb_meters s
      ON s.meterid = r.meter_id
where s.meterid is null;
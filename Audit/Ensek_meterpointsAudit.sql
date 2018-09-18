--Updated Meterpoints SQL
insert into ref_meterpoints_audit
select
		s.account_id,
		s.meter_point_id,
		s.meterpointnumber,
		nullif(s.associationstartdate,'')::timestamp,
		nullif(s.associationenddate,'')::timestamp,
		nullif(s.supplystartdate,'')::timestamp,
		nullif(s.supplyenddate,'')::timestamp,
		s.issmart,
		s.issmartcommunicating,
		s.meterpointtype,
		'u', current_timestamp
from aws_s3_ensec_api_extracts.cdb_meterpoints s
       inner join ref_meterpoints r
       		ON s.meter_point_id = r.meter_point_id
where (s.meterpointnumber !=r.meterpointnumber
		or nullif(s.associationstartdate,'')::timestamp!=r.associationstartdate
		or nullif(s.associationenddate,'')::timestamp!=r.associationenddate
		or nullif(s.supplystartdate,'')::timestamp!=r.supplystartdate
		or nullif(s.supplyenddate,'')::timestamp!=r.supplyenddate
		or s.issmart != r.issmart
		or s.issmartcommunicating != r.issmartcommunicating
		or s.meterpointtype!=r.meterpointtype);
-- [2018-09-17 14:53:05] completed in 4 m 24 s 50 ms
-- found 0 rows to update

-- New Meterpoints SQL
insert into ref_meterpoints_audit
SELECT
		s.account_id ,
		s.meter_point_id,
  	s.meterpointnumber,
		nullif(s.associationstartdate,'')::timestamp,
		nullif(s.associationenddate,'')::timestamp,
		nullif(s.supplystartdate,'')::timestamp,
		nullif(s.supplyenddate,'')::timestamp,
		s.issmart,
		s.issmartcommunicating,
		s.meterpointtype,
		'n',
		current_timestamp
from aws_s3_ensec_api_extracts.cdb_meterpoints s
       left outer join ref_meterpoints r
					ON r.meter_point_id = s.meter_point_id
where r.meter_point_id is null;
-- 36914 rows affected in 4 m 24 s 426 ms


-- Deleted Meterpoints SQL
insert into ref_meterpoints_audit
SELECT
		r.account_id,
		r.meter_point_id,
		r.meterpointnumber,
		r.associationstartdate,
		r.associationenddate,
		r.supplystartdate,
		r.supplyenddate,
		r.issmart,
		r.issmartcommunicating,
		r.meterpointtype,
		'd', current_timestamp
from ref_meterpoints r
       left outer join aws_s3_ensec_api_extracts.cdb_meterpoints s
        ON s.meter_point_id = r.meter_point_id
where s.meter_point_id is null;
-- completed in 3 m 21 s 740 ms
-- found 0 deleted rows
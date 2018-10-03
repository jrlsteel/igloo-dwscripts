--
-- select count(*) from ref_readings; --259780
-- select count(*) from (
-- select
-- account_id,meter_point_id,
-- --id,
-- reading_id
-- --reading_registerid
-- from ref_readings
-- group by
-- account_id,meter_point_id,
-- --id,
-- reading_id
-- --reading_registerid
-- ); --231327

-- create ref table
create table ref_readings
(
	account_id bigint,
	meter_point_id bigint encode delta,
	reading_registerid bigint,
	reading_id bigint,
	id bigint,
	meterpointid bigint,
	datetime timestamp,
	createddate timestamp,
	meterreadingsource varchar(255),
	readingtype varchar(255),
	reading_value double precision
)
;

alter table ref_readings owner to igloo
;
-- create audit table
create table ref_readings_audit
(
	account_id bigint,
	meter_point_id bigint encode delta,
	reading_registerid bigint,
	reading_id bigint,
	id bigint,
	meterpointid bigint,
	datetime timestamp,
	createddate timestamp,
	meterreadingsource varchar(255),
	readingtype varchar(255),
	reading_value double precision,
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_readings_audit owner to igloo
;

-- New or update for audit
insert into ref_readings_audit (
select
		s.account_id,
		s.meter_point_id,
		s.reading_registerid,
		s.reading_id,
		s.id,
		s.meterpointid,
		cast (nullif(s.datetime,'') as timestamp) as datetime,
		cast (nullif(s.createddate, '') as timestamp) as createddate,
		s.meterreadingsource,
		s.readingtype,
		s.reading_value,
		case when r.reading_id is null then 'n' else 'u' end etlchangetype,
		current_timestamp etlchange
from aws_s3_ensec_api_extracts.cdb_readings s
       left outer join ref_readings r
       		on r.account_id = s.account_id
       		and r.meter_point_id = s.meter_point_id
       		and r.reading_id = s.reading_id
where cast (nullif(s.datetime, '') as timestamp) != r.datetime
	  or cast (nullif(s.createddate, '') as timestamp) != r.createddate
		or s.meterreadingsource != r.meterreadingsource
		or s.reading_registerid != r.reading_registerid
		or s.readingtype != r.readingtype
		or s.reading_value != r.reading_value
		or s.meterpointid != r.meterpointid
	  or s.id != r.id
	  or s.reading_registerid != r.reading_registerid
		or r.reading_id is null);

-- overwrite ref
insert into ref_readings (
select
		s.account_id,
		s.meter_point_id,
		s.reading_registerid,
		s.reading_id,
		s.id,
		s.meterpointid,
		cast (nullif(s.datetime, '') as timestamp) as datetime,
		cast (nullif(s.createddate, '') as timestamp) as createddate,
		s.meterreadingsource,
		s.readingtype,
		s.reading_value
from aws_s3_ensec_api_extracts.cdb_readings s);


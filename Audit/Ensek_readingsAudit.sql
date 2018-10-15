
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
	meter_point_id bigint,
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
-- insert into ref_readings_audit (
select
		cast (s.account_id as bigint) as account_id,
		cast (s.meter_point_id as bigint) as meter_point_id,
		cast (s.reading_registerid as bigint) as reading_registerid,
		cast (s.reading_id as bigint) as reading_id,
		cast (s.id as bigint) as id,
		cast (s.meterpointid as bigint) as meterpointid,
		cast (nullif(s.datetime,'') as timestamp) as datetime,
		cast (nullif(s.createddate, '') as timestamp) as createddate,
		s.meterreadingsource,
		s.readingtype,
		cast (s.reading_value as double precision) as reading_value,
		case when r.reading_id is null then 'n' else 'u' end as etlchangetype,
		current_timestamp as etlchange
from aws_s3_ensec_api_extracts.cdb_stagereadings s
       left outer join ref_readings r
       		on r.account_id = cast (s.account_id as bigint)
       		and r.meter_point_id = cast (s.meter_point_id as bigint)
       		and r.reading_id = cast (s.reading_id as bigint)
where cast (nullif(s.datetime, '') as timestamp) != r.datetime
	    or cast (nullif(s.createddate, '') as timestamp) != r.createddate
		or s.meterreadingsource != r.meterreadingsource
		or cast (s.reading_registerid as bigint) != r.reading_registerid
		or s.readingtype != r.readingtype
		or cast (s.reading_value as double precision) != r.reading_value
		or cast (s.meterpointid as bigint) != r.meterpointid
	    or cast (s.id as bigint) != r.id
	    or cast (s.reading_registerid as bigint) != r.reading_registerid
	    or r.reading_id is null 
-- )
;

-- overwrite ref
insert into ref_readings (
select
		cast (s.account_id as bigint) as account_id,
		cast (s.meter_point_id as bigint) as meter_point_id ,
		cast (s.reading_registerid as bigint) as reading_registerid ,
		cast (s.reading_id as bigint) as reading_id ,
		cast (s.id as bigint) as id ,
		cast (s.meterpointid as bigint) as meterpointid ,
		cast (nullif(s.datetime,'') as timestamp) as datetime,
		cast (nullif(s.createddate, '') as timestamp) as createddate,
		s.meterreadingsource,
		s.readingtype,
		cast (s.reading_value as double precision)
from aws_s3_ensec_api_extracts.cdb_readings s);


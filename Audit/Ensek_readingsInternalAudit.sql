--drop table ref_readings_internal;
drop table igloosense."public".ref_readings_internal;
create table if not exists ref_readings_internal
(
	account_id bigint,
	meter_point_id bigint,
	meter_id bigint,
	meter_reading_id bigint,
	register_id bigint,
	register_reading_id bigint,
	billable boolean,
	haslivecharge boolean,
	hasregisteradvance boolean,
	meterpointnumber bigint,
	meterpointtype varchar(1),
	meterreadingcreateddate timestamp,
	meterreadingdatetime timestamp,
	meterreadingsourceuid varchar(255),
	meterreadingstatusuid varchar(255),
	meterreadingtypeuid varchar(255),
	meterserialnumber varchar(255),
	readingvalue double precision,
	registerreference varchar(255),
	required boolean
);

alter table ref_readings_internal owner to igloo;

-- drop table ref_readings_internal_audit;
create table if not exists ref_readings_internal_audit
(
	account_id bigint,
	meter_point_id bigint,
	meter_id bigint,
	meter_reading_id bigint,
	register_id bigint,
	register_reading_id bigint,
	billable boolean,
	haslivecharge boolean,
	hasregisteradvance boolean,
	meterpointnumber bigint,
	meterpointtype varchar(1),
	meterreadingcreateddate timestamp,
	meterreadingdatetime timestamp,
	meterreadingsourceuid varchar(255),
	meterreadingstatusuid varchar(255),
	meterreadingtypeuid varchar(255),
	meterserialnumber varchar(255),
	readingvalue double precision,
	registerreference varchar(255),
	required boolean,
	etlchangetype varchar(1),
	etlchange timestamp
);

alter table ref_readings_internal_audit owner to igloo;

-- New or Updated data for audit
-- insert into ref_readings_internal_audit (
select
  cast (s.accountid as bigint) as accountid,
	cast (s.meterpointid as bigint) as meterpointid,
	cast (s.meterid as bigint) as meterid,
	cast (s.meterreadingid as bigint) as meterreadingid,
	cast (s.registerid as bigint) as registerid,
	cast (s.registerreadingid as bigint) as registerreadingid,
	cast (cast (case when s.billable = 'True' then 1 else 0 end as int) as boolean) as billable,
	cast (cast (case when s.haslivecharge = 'True' then 1 else 0 end as int) as boolean) as haslivecharge,
	cast (cast (case when s.hasregisteradvance = 'True' then 1 else 0 end as int) as boolean) as hasregisteradvance,
	cast (s.meterpointnumber as bigint) as meterpointnumber,
	trim(s.meterpointtype) as meterpointtype,
	cast (s.meterreadingcreateddate as timestamp) meterreadingcreateddate,
	cast (s.meterreadingdatetime as timestamp) meterreadingdatetime,
	trim(s.meterreadingsourceuid) as meterreadingsourceuid,
	trim(s.meterreadingstatusuid) as meterreadingstatusuid,
	trim(s.meterreadingtypeuid) as meterreadingtypeuid,
	trim(s.meterserialnumber) as meterserialnumber,
	cast (s.readingvalue as double precision) as readingvalue,
	trim(s.registerreference) as registerreference,
	cast (cast (case when s.required = 'True' then 1 else 0 end as int) as boolean) as required,
	case when r.register_reading_id  is null then 'n' else 'u' end etlchangetype,
	current_timestamp etlchange
from aws_s3_ensec_api_extracts.cdb_stagereadingsinternal s
      left outer join ref_readings_internal r
      		on r.account_id = cast (s.accountid as bigint)
      		and r.meter_point_id = cast (s.meterpointid as bigint)
      		and r.meter_id = cast (s.meterid as bigint)
      		and r.meter_reading_id = cast (s.meterreadingid as bigint)
					and r.register_id = cast (s.registerid as bigint)
					and r.register_reading_id = cast (s.registerreadingid as bigint)
where cast (cast (case when s.billable = 'True' then 1 else 0 end as int) as boolean) != r.billable
			or cast (cast (case when s.haslivecharge = 'True' then 1 else 0 end as int) as boolean) != r.haslivecharge
			or cast (cast (case when s.hasregisteradvance = 'True' then 1 else 0 end as int) as boolean) != r.hasregisteradvance
			or cast (s.meterpointnumber as bigint) != r.meterpointnumber
			or trim(s.meterpointtype) != trim(r.meterpointtype)
			or cast (s.meterreadingcreateddate as timestamp) != r.meterreadingcreateddate
			or cast (s.meterreadingdatetime as timestamp) != r.meterreadingdatetime
			or trim(s.meterreadingsourceuid) != trim(r.meterreadingsourceuid)
			or trim(s.meterreadingstatusuid) != trim(r.meterreadingstatusuid)
			or trim(s.meterserialnumber) != trim(r.meterserialnumber)
			or cast (s.readingvalue as double precision) != r.readingvalue
			or trim(s.registerreference) != trim(r.registerreference)
      or cast (cast (case when s.required = 'True' then 1 else 0 end as int) as boolean) != r.required
			or r.register_reading_id is null
-- )
;

-- Overwrite ref table
-- insert into ref_readings_internal (
select cast (s.accountid as bigint) as accountid,
	cast (s.meterpointid as bigint) as meterpointid,
	cast (s.meterid as bigint) as meterid,
	cast (s.meterreadingid as bigint) as meterreadingid,
	cast (s.registerid as bigint) as registerid,
	cast (s.registerreadingid as bigint) as registerreadingid,
	cast (cast (case when s.billable = 'True' then 1 else 0 end as int) as boolean) as billable,
	cast (cast (case when s.haslivecharge = 'True' then 1 else 0 end as int) as boolean) as haslivecharge,
	cast (cast (case when s.hasregisteradvance = 'True' then 1 else 0 end as int) as boolean) as hasregisteradvance,
	cast (s.meterpointnumber as bigint) as meterpointnumber,
	trim(s.meterpointtype) as meterpointtype,
	cast (s.meterreadingcreateddate as timestamp) meterreadingcreateddate,
	cast (s.meterreadingdatetime as timestamp) meterreadingdatetime,
	trim(s.meterreadingsourceuid) as meterreadingsourceuid,
	trim(s.meterreadingstatusuid) as meterreadingstatusuid,
	trim(s.meterreadingtypeuid) as meterreadingtypeuid,
	trim(s.meterserialnumber) as meterserialnumber,
	cast (s.readingvalue as double precision) as readingvalue,
	trim(s.registerreference) as registerreference,
	cast (cast (case when s.required = 'True' then 1 else 0 end as int) as boolean) as required
	from aws_s3_ensec_api_extracts.cdb_stagereadingsinternal s
--     )
;
--TESTING--
-- test new rows
delete from ref_readings_internal where account_id=2340; -- 44 rows
-- test updated rows
update ref_readings_internal set readingvalue = 100 where account_id = 2435;-- 45 rows
